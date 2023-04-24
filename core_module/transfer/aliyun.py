import asyncio
import itertools
import re
import warnings
from collections import defaultdict
from enum import Flag
from pathlib import PurePosixPath
from typing import Iterable

import aiohttp
import tenacity
from aiohttp_client_cache import CacheBackend
from ordered_set import OrderedSet
from tenacity import retry, retry_if_exception_message, stop_after_attempt

from core_module.base.driver import DirectoryItem, DirectoryDriver, HttpResponseErrorHandler, TransferDriver, \
    AuthDriver
from core_module.base.task import TransferTask, TransferFlag
from tool_module.aio_throttled_cached_session import ThrottledCachedSession
from utils.async_io import class_decorator
from utils.common import rand_b64

warnings.simplefilter('always', UserWarning)


class AliResponseErrorHandler(HttpResponseErrorHandler):
    @staticmethod
    async def async_handle(resp):
        txt = await resp.text()
        if not AliResponseErrorHandler.success(resp.status):
            raise AliResponseErrorHandler.id(None, txt).exception


class AliAuthDriver(AuthDriver):
    regex = re.compile(r'https?://www\.aliyundrive\.com/s/.*?')
    batch_optimized = False

    def __init__(self, refresh_token: str = None, *, access_token: str = None, user_drive_id: str = None,
                 aio_session: aiohttp.ClientSession = None):
        super().__init__()
        if aio_session:
            self.s = aio_session
        else:
            self.s = self._build_session(aio_session=ThrottledCachedSession, headers={
                'authority': 'api.aliyundrive.com',
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
                'content-type': 'application/json;charset=UTF-8',
                'dnt': '1',
                'origin': 'https://www.aliyundrive.com',
                'pragma': 'no-cache',
                'referer': 'https://www.aliyundrive.com/',
                'sec-ch-ua': '"Chromium";v="112", "Microsoft Edge";v="112", "Not:A-Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.48',
                'x-canary': 'client=web,app=share,version=v2.3.1',
                'x-device-id': rand_b64(25)
            }, cache=CacheBackend(allowed_methods=('GET', 'HEAD', 'POST'), allowed_codes=(200, 201, 202), limit=10))

        self._access_token = access_token
        self._refresh_token = refresh_token
        self._drive_id = user_drive_id

    @property
    def persistent_data(self):
        data = {
            'access_token': self._access_token,
            'refresh_token': self._refresh_token,
            'drive_id': self._drive_id
        }
        return data

    @persistent_data.setter
    def persistent_data(self, value):
        self._access_token = value['access_token']
        self._refresh_token = value['refresh_token']
        self._drive_id = value['drive_id']

    @staticmethod
    def require_drive_id(func):
        def ensure_req(self, *args, **kwargs):
            if self._drive_id is None:
                self._drive_id = self._loop.run_until_complete(self._get_user_drive_id())

        return class_decorator(ensure_req, func)

    @staticmethod
    def require_access_token(func):
        def ensure_req(self, *args, **kwargs):
            if self._access_token is None:
                self._loop.run_until_complete(self.async_refresh_access_token())

        return class_decorator(ensure_req, func)

    @staticmethod
    def require_refresh_token(func):
        def ensure_req(self, *args, **kwargs):
            if self._refresh_token is None:
                raise ValueError("Need refresh token.")

        return class_decorator(ensure_req, func)

    @property
    @require_access_token
    def auth_header(self):
        return {'authorization': f'Bearer {self._access_token}'}

    @property
    @require_drive_id
    def drive_id(self):
        return self._drive_id

    @property
    @require_access_token
    def access_token(self):
        return self._access_token

    @property
    @require_refresh_token
    def refresh_token(self):
        return self._refresh_token

    @require_refresh_token
    async def async_refresh_access_token(self):
        if self.refresh_token is None:
            return False
        url = 'https://api.aliyundrive.com/token/refresh'
        json_data = {
            'refresh_token': self.refresh_token,
        }
        async with self.s.post(url, json=json_data) as resp:
            await AliResponseErrorHandler.async_handle(resp)
            js = await resp.json()
            self._access_token = js['access_token']
            self._refresh_token = js['refresh_token']
            return True

    @staticmethod
    def retry_on_token_expire(func):
        def wrapper(self, *args, **kwargs):
            def refresh_access_token(retry_info: tenacity.RetryCallState):
                if not retry_info.attempt_number == 1:
                    self._loop.run_until_complete(self.async_refresh_access_token())

            r = retry(retry=retry_if_exception_message(match='expire'),
                      before=refresh_access_token,
                      stop=stop_after_attempt(2))
            return r(func)(self, *args, **kwargs)

        return wrapper

    @retry_on_token_expire
    @require_access_token
    async def _get_user_drive_id(self):
        url = 'https://api.aliyundrive.com/adrive/v2/user/get'
        async with self.s.post(url, headers=self.auth_header, json={}) as resp:
            await AliResponseErrorHandler.async_handle(resp)
            js = await resp.json()
            return js['default_drive_id']

    async def close(self):
        if not self.s.closed:
            await self.s.close()


class AliTransferTask(TransferTask):
    def __init__(self, url: str, target_dir: str | PurePosixPath = PurePosixPath('/'), code: int | str = None,
                 flags: Flag = TransferFlag.create_parents):
        super().__init__(url, target_dir, code, flags)


class AliDirectoryItem(DirectoryItem):
    def __init__(self, path: str | PurePosixPath, is_dir: bool, cloud_id: str = None, cloud_parent_id: str = None,
                 available: bool = None):
        self.path = path if isinstance(path, PurePosixPath) else PurePosixPath(path)
        self.is_dir = is_dir
        self.cloud_id = cloud_id
        self.cloud_parent_id = cloud_parent_id
        self.available = available
        super().__init__(self.calc_id(self.path, is_dir),
                         self.calc_id(self.path.parent, True),
                         self.path.name)

    @staticmethod
    def calc_id(path: PurePosixPath | str, is_dir: bool):
        if not isinstance(path, PurePosixPath):
            path = PurePosixPath(path)
        return path.as_posix() + '//' + str(is_dir)

    @staticmethod
    def double_id(path: PurePosixPath | str, is_dir: bool):
        path = path if isinstance(path, PurePosixPath) else PurePosixPath(path)
        uni_id = AliDirectoryItem.calc_id(path.as_posix(), is_dir)
        parent_uni_id = AliDirectoryItem.calc_id(path.parent.as_posix(), is_dir)
        return uni_id, parent_uni_id


class AliDirectoryDriver(DirectoryDriver):
    regex = re.compile(r'https?://www\.aliyundrive\.com/s/.*?')
    batch_optimized = False

    def __init__(self, refresh_token: str = None, *, access_token: str = None, user_drive_id: str = None,
                 aio_session: aiohttp.ClientSession = None, auth_driver: AliAuthDriver = None):
        super().__init__()
        if auth_driver:
            self.auth_driver = auth_driver

        else:
            self.auth_driver = AliAuthDriver(access_token=access_token,
                                             refresh_token=refresh_token,
                                             user_drive_id=user_drive_id,
                                             aio_session=aio_session)
        self.s = self.auth_driver.s
        self._mkdir_mutex = defaultdict(asyncio.Lock)
        self._ls_mutex = defaultdict(asyncio.Lock)


    @staticmethod
    def retry_on_access_token_expire(func):
        def wrapper(self, *args, **kwargs):
            def refresh_access_token(retry_info: tenacity.RetryCallState):
                if not retry_info.attempt_number == 1:
                    self._loop.run_until_complete(self.auth_driver.async_refresh_access_token())

            r = retry(retry=retry_if_exception_message(match='expire'),
                      before=refresh_access_token,
                      stop=stop_after_attempt(2))
            return r(func)(self, *args, **kwargs)

        return wrapper

    @retry_on_access_token_expire
    async def _ls(self, dir_id='root'):
        await self._ls_mutex[dir_id].acquire()
        url = 'https://api.aliyundrive.com/adrive/v3/file/list'
        json_data = {
            'drive_id': self.auth_driver.drive_id,
            'parent_file_id': dir_id,
            'order_by': 'updated_at',
            'order_direction': 'DESC',
        }
        async with self.s.cache_disabled():
            async with self.s.post(url, json=json_data, headers=self.auth_driver.auth_header) as resp:
                await AliResponseErrorHandler.async_handle(resp)
                js = await resp.json()
                self._ls_mutex[dir_id].release()
                return js['items']

    async def ls(self, path: PurePosixPath | str = PurePosixPath('/'), renew=False):
        path = path if isinstance(path, PurePosixPath) else PurePosixPath(path)
        last_dir_cache = None
        for part, partial_path in itertools.zip_longest(path.parts, (*reversed(path.parents), path),
                                                        fillvalue=PurePosixPath('/')):
            uni_id = AliDirectoryItem.calc_id(partial_path, True)
            if part == '/':
                cur_cloud_id = 'root'
            else:
                cur_item = self.cache_get(uni_id, cache=last_dir_cache)
                if not cur_item:
                    return None
                cur_cloud_id = cur_item.cloud_id

            dir_cache = self.cache_ls(uni_id)
            if dir_cache is not None:
                last_dir_cache = dir_cache
                continue

            ls_result = await self._ls(cur_cloud_id)
            dir_items = OrderedSet(
                AliDirectoryItem(partial_path.joinpath(item['name']).as_posix(),
                                 True if item['type'] == 'folder' else False,
                                 item['file_id'],
                                 item['parent_file_id'],
                                 True if item['status'] == 'available' else False) for item in ls_result)
            self.cache_add(uni_id, dir_items, renew)
            last_dir_cache = dir_items

        return last_dir_cache

    async def exists(self, dir_item: AliDirectoryItem | str):
        cached_exists = self.cache_exists(dir_item)
        if cached_exists is not None:
            return cached_exists
        await self.ls(dir_item.path.parent)
        return self.cache_exists(dir_item)

    @retry_on_access_token_expire
    async def _mkdir(self, parent_file_id, name):
        await self._mkdir_mutex[parent_file_id+'.'+name].acquire()
        url = 'https://api.aliyundrive.com/adrive/v2/file/createWithFolders'
        json_data = {
            'drive_id': self.auth_driver.drive_id,
            'parent_file_id': parent_file_id,
            'name': name,
            'check_name_mode': 'refuse',
            'type': 'folder',
        }
        async with self.s.post(url, headers=self.auth_driver.auth_header, json=json_data) as resp:
            await AliResponseErrorHandler.async_handle(resp)
            js = await resp.json()
            self._mkdir_mutex[parent_file_id+'.'+name].release()
            return js

    async def mkpath(self, path: PurePosixPath | str = PurePosixPath('/')):
        path = path if isinstance(path, PurePosixPath) else PurePosixPath(path)
        dir_item = AliDirectoryItem(path.as_posix(), True)
        cached_item = await self.exists(dir_item)
        if cached_item:
            return cached_item

        last_cloud_id = 'root'
        for part, partial_path in itertools.zip_longest(path.parts, (*reversed(path.parents), path),
                                                        fillvalue=PurePosixPath('/')):
            if partial_path.as_posix() == '/':
                continue

            dir_item = AliDirectoryItem(partial_path, True)
            cached_item = await self.exists(dir_item)
            if cached_item:
                last_cloud_id = cached_item.cloud_id
                continue

            item = await self._mkdir(last_cloud_id, part)
            cached_item = AliDirectoryItem(partial_path,
                                           True,
                                           item['file_id'],
                                           item['parent_file_id'],
                                           True)
            self.cache_add(AliDirectoryItem.calc_id(partial_path.parent.as_posix(), True), cached_item)
            last_cloud_id = item['file_id']

        return cached_item

    async def close(self):
        if not self.s.closed:
            await self.s.close()


class AliTransferDriver(TransferDriver):
    regex = re.compile(r'https?://www\.aliyundrive\.com/s/.*?')
    batch_optimized = True

    def __init__(self, refresh_token: str = None, *, access_token: str = None, user_drive_id: str = None,
                 aio_session: aiohttp.ClientSession = None, auth_driver: AliAuthDriver = None):
        super().__init__()
        if auth_driver:
            self.auth_driver = auth_driver

        else:
            self.auth_driver = AliAuthDriver(access_token=access_token,
                                             refresh_token=refresh_token,
                                             user_drive_id=user_drive_id,
                                             aio_session=aio_session)
        self.s = self.auth_driver.s
        self.s.set_regex_rate_limit(re.compile(r'https?://api\.aliyundrive\.com/v2/file/list_by_share'),
                                    10 / 60)
        self.s.set_host_rate_limit('api.aliyundrive.com', 30 / 60)
        self._dir_driver = AliDirectoryDriver(auth_driver=self.auth_driver)
        self._task_mutex = defaultdict(asyncio.Lock)

    @staticmethod
    def retry_on_access_token_expire(func):
        def wrapper(self, *args, **kwargs):
            def refresh_access_token(retry_info: tenacity.RetryCallState):
                if not retry_info.attempt_number == 1:
                    self._loop.run_until_complete(self.auth_driver.async_refresh_access_token())

            r = retry(retry=retry_if_exception_message(match='expire'),
                      before=refresh_access_token,
                      stop=stop_after_attempt(2))
            return r(func)(self, *args, **kwargs)
        return wrapper

    @property
    def persistent_data(self):
        data = {
            'auth_driver_persistent_data': self.auth_driver.persistent_data,
        }
        return data

    @staticmethod
    def share_url_get_id(url):
        match = re.search(r'https?://www\.aliyundrive\.com/s/(.*)\??', url)
        return match.group(1) or None

    @persistent_data.setter
    def persistent_data(self, value):
        self.auth_driver.persistent_data = value['auth_driver_persistent_data']

    @retry_on_access_token_expire
    async def _get_share_token(self, share_id: str, code: str = None):
        if share_id is None:
            return None
        url = 'https://api.aliyundrive.com/v2/share_link/get_share_token'
        json_data = {
            'share_id': share_id,
            'share_pwd': code if code else ""}

        async with self.s.post(url, json=json_data) as resp:
            await AliResponseErrorHandler.async_handle(resp)
            js = await resp.json()
            return js['share_token']

    @retry_on_access_token_expire
    async def _batch_get_share_token_internal(self,
                                              share_ids: Iterable[str],
                                              share_codes: Iterable[str | None]):
        share_tokens = []
        requests = []
        for i, (share_id, share_code) in \
                enumerate(zip(share_ids, share_codes)):
            if share_id is None:
                share_tokens.append(None)
                continue
            share_tokens.append(True)
            request = {
                'body': {
                    'share_id': share_id,
                    'share_pwd': share_code or ""
                },
                'headers': {
                    'Content-Type': 'application/json',
                },
                'id': str(i),
                'method': 'POST',
                'url': '/share_link/get_share_token',
            }
            requests.append(request)

        if not requests:
            return [None]

        url = 'https://api.aliyundrive.com/adrive/v2/batch'
        json_data = {
            'requests': requests,
            'resource': 'share_link',
        }
        async with self.s.post(url, json=json_data, headers=self.auth_driver.auth_header) as resp:
            await AliResponseErrorHandler.async_handle(resp)
            js = await resp.json()
            resp_iter = iter(js['responses'])
            for i, resp_slot in enumerate(share_tokens):
                if resp_slot:
                    try:
                        share_tokens[i] = next(resp_iter)['body']['share_token']
                    except KeyError:
                        share_tokens[i] = None
            return share_tokens

    async def _get_share_info(self, share_id: str, share_token: str, parent_file_id='root'):
        if share_id is None or share_token is None:
            return None
        url = 'https://api.aliyundrive.com/adrive/v2/file/list_by_share'
        json_data = {
            'share_id': share_id,
            'parent_file_id': parent_file_id,
            'order_by': 'name',
            'order_direction': 'DESC'}

        async with self.s.post(url, json=json_data, headers={'x-share-token': share_token}) as resp:
            await AliResponseErrorHandler.async_handle(resp)
            js = await resp.json()
            return js['items']

    @retry_on_access_token_expire
    async def _batch_transfer_internal(self,
                                       share_tokens: Iterable[str],
                                       file_ids: Iterable[str],
                                       share_ids: Iterable[str],
                                       to_drive_ids: Iterable[str],
                                       to_parent_file_ids: Iterable[str] = itertools.repeat('root'),
                                       auto_renames: Iterable[bool] = itertools.repeat(False)):
        responses = []
        requests = []
        for i, (share_token, file_id, share_id, to_drive_id, to_parent_file_id, auto_rename) in \
                enumerate(zip(share_tokens, file_ids, share_ids, to_drive_ids, to_parent_file_ids, auto_renames)):
            if any(value is None for value in
                   (share_token, file_id, share_id, to_drive_id, to_parent_file_id, auto_rename)):
                responses.append(None)
                continue
            responses.append(True)
            request = {
                'body': {
                    'file_id': file_id,
                    'share_id': share_id,
                    'auto_rename': auto_rename,
                    'to_parent_file_id': to_parent_file_id,
                    'to_drive_id': to_drive_id,
                },
                'headers': {
                    'Content-Type': 'application/json',
                    'x-share-token': share_token
                },
                'id': str(i),
                'method': 'POST',
                'url': '/file/copy'}
            requests.append(request)

        if not requests:
            return [None]

        url = 'https://api.aliyundrive.com/adrive/v2/batch'
        json_data = {
            'requests': requests,
            'resource': 'file',
        }
        async with self.s.post(url, json=json_data, headers=self.auth_driver.auth_header) as resp:
            await AliResponseErrorHandler.async_handle(resp)
            js = await resp.json()
            resp_iter = iter(js['responses'])
            for i, resp_slot in enumerate(responses):
                if resp_slot:
                    responses[i] = next(resp_iter)
            return responses

    async def _batch_transfer(self, *tasks: TransferTask):
        try:
            ls_futures = []
            for task in tasks:
                task.start()
                ls_futures.append(self._loop.create_task(self._dir_driver.ls(task.target_dir)))

            mkpath_futures = []
            for task in tasks:
                mkpath_futures.append(self._loop.create_task(self._dir_driver.mkpath(task.target_dir)))

            share_tokens = await self._batch_get_share_token_internal(
                [self.share_url_get_id(task.url) for task in tasks],
                [task.code for task in tasks])

            info_futures = []
            for task, token in zip(tasks, share_tokens):
                info_futures.append(
                    self._loop.create_task(self._get_share_info(self.share_url_get_id(task.url), token)))

            transfer_params = []
            for task, mkpath_future, token, info_future in zip(tasks,
                                                               mkpath_futures,
                                                               share_tokens,
                                                               info_futures):
                transfer_param = (None,) * 5

                mkpath = await mkpath_future
                info = await info_future
                if not mkpath:
                    warnings.warn(f'创建文件夹{task.target_dir}出错')
                    task.finish(False)
                    transfer_params.append(transfer_param)
                    continue
                if not token:
                    warnings.warn(f'{task}未获取到token')
                    task.finish(False)
                    transfer_params.append(transfer_param)
                    continue
                if not info_future:
                    warnings.warn(f'{task}未获取到文件信息')
                    task.finish(False)
                    transfer_params.append(transfer_param)
                    continue

                for info_item in info:
                    try:
                        p = mkpath.path.joinpath(info_item['name'])
                        dir_item = AliDirectoryItem(p.as_posix(),
                                                    info_item['type'] == 'folder' or False)
                        if await self._dir_driver.exists(dir_item):
                            warnings.warn(f'{p.as_posix()}已存在，跳过转存')
                            task.finish(True)
                            transfer_params.append(transfer_param)
                            continue
                        transfer_param = (token,
                                          info_item['file_id'],
                                          info_item['share_id'],
                                          self.auth_driver.drive_id,
                                          mkpath.cloud_id,
                                          TransferFlag.rename in task.flags)
                    except AttributeError:
                        pass
                    transfer_params.append(transfer_param)

            transfer_responses = await self._batch_transfer_internal(*zip(*transfer_params))

            for task, resp in zip(tasks, transfer_responses):
                if task.finished:
                    continue
                if resp and AliResponseErrorHandler.success(resp['status']):
                    task.finish(True)
                else:
                    warnings.warn(f'任务 {task} 转存失败：{resp}，此警告可能源自某一已打印过的警告')
                    task.finish(False)

        except Exception as e:
            for task in tasks:
                if not task.finished:
                    task.finish(False)
            raise e

        return tasks

    async def transfer(self, *tasks: AliTransferTask):
        async_task = self._loop.create_task(self._batch_transfer(*tasks))
        return [async_task]

    async def close(self):
        if not self.s.closed:
            await self.s.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._loop.run_until_complete(self.close())
