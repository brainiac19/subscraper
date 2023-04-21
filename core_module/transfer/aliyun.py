import asyncio
import itertools
import re
import warnings
from collections import defaultdict
from enum import Flag
from pathlib import PurePosixPath
from typing import Iterable

import aiohttp
from aiohttp_client_cache import CacheBackend
from ordered_set import OrderedSet

from core_module.base.driver import DirectoryItem, DirectoryDriver, HttpResponseErrorHandler, TransferDriver
from core_module.base.task import TransferTask, TransferFlag
from tool_module.aio_throttled_cached_session import ThrottledCachedSession
from utils.common import rand_b64


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

    def __init__(self, auth_token: str, *, user_drive_id: str = None, aio_session: aiohttp.ClientSession = None):
        super().__init__()
        if aio_session:
            self._s = aio_session
        else:
            self._s = self._build_session(aio_session=ThrottledCachedSession, headers={
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
            }, cache=CacheBackend(allowed_methods=('GET', 'HEAD', 'POST'), limit=10))
            self._s.set_host_rate_limit('api.aliyundrive.com', 5)

        self._auth_token = auth_token
        self._mkpath_mutex = asyncio.Lock()
        self.drive_id = user_drive_id if user_drive_id else self._loop.run_until_complete(self._get_user_drive_id())


    @property
    def _auth_header(self):
        return {'authorization': f'Bearer {self._auth_token}'}

    async def _get_user_drive_id(self):
        url = 'https://api.aliyundrive.com/adrive/v2/user/get'
        async with self._s.post(url, headers=self._auth_header, json={}) as resp:
            txt = await resp.text()
            if not HttpResponseErrorHandler.success(resp.status):
                raise HttpResponseErrorHandler.id(resp.status, txt).exception
            js = await resp.json()
            return js['default_drive_id']

    async def _ls(self, dir_id='root'):
        url = 'https://api.aliyundrive.com/adrive/v3/file/list'
        json_data = {
            'drive_id': self.drive_id,
            'parent_file_id': dir_id,
            'order_by': 'updated_at',
            'order_direction': 'DESC',
        }
        async with self._s.cache_disabled():
            async with self._s.post(url, json=json_data, headers=self._auth_header) as resp:
                txt = await resp.text()
                if not HttpResponseErrorHandler.success(resp.status):
                    raise HttpResponseErrorHandler.id(resp.status, txt).exception
                js = await resp.json()
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

    async def _mkdir(self, parent_file_id, name):
        url = 'https://api.aliyundrive.com/adrive/v2/file/createWithFolders'
        json_data = {
            'drive_id': self.drive_id,
            'parent_file_id': parent_file_id,
            'name': name,
            'check_name_mode': 'refuse',
            'type': 'folder',
        }
        async with self._s.post(url, headers=self._auth_header, json=json_data) as resp:
            txt = await resp.text()
            if not HttpResponseErrorHandler.success(resp.status):
                raise HttpResponseErrorHandler.id(resp.status, txt).exception
            js = await resp.json()
            return js

    async def mkpath(self, path: PurePosixPath | str = PurePosixPath('/')):
        path = path if isinstance(path, PurePosixPath) else PurePosixPath(path)
        await self._mkpath_mutex.acquire()
        dir_item = AliDirectoryItem(path.as_posix(), True)
        cached_item = await self.exists(dir_item)
        if cached_item:
            self._mkpath_mutex.release()
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

        self._mkpath_mutex.release()
        return cached_item

    async def close(self):
        if not self._s.closed:
            await self._s.close()


class AliTransferDriver(TransferDriver):
    regex = re.compile(r'https?://www\.aliyundrive\.com/s/.*?')
    batch_optimized = True

    def __init__(self, auth_token: str, aio_session: aiohttp.ClientSession = None):
        super().__init__()
        if aio_session:
            self._s = aio_session
        else:
            self._s = self._build_session(aio_session=ThrottledCachedSession, headers={
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
            }, cache=CacheBackend(allowed_methods=('GET', 'HEAD', 'POST'), limit=10))
            self._s.set_host_rate_limit('api.aliyundrive.com', 5)

        self._auth_token = auth_token
        self._dir_driver = AliDirectoryDriver(self._auth_token, aio_session=self._s)
        self._task_mutex = defaultdict(asyncio.Lock)

    @property
    def _auth_header(self):
        return {'authorization': f'Bearer {self._auth_token}'}

    async def _get_share_token(self, share_url: str, code: str = None):
        url = 'https://api.aliyundrive.com/v2/share_link/get_share_token'
        json_data = {
            'share_id': re.search(r'https?://www\.aliyundrive\.com/s/(.*)\??', share_url).group(1),
            'share_pwd': code if code else ""}

        async with self._s.post(url, json=json_data) as resp:
            txt = await resp.text()
            if not HttpResponseErrorHandler.success(resp.status):
                raise HttpResponseErrorHandler.id(resp.status, txt).exception
            js = await resp.json()
            return js['share_token']

    async def _get_share_info(self, share_url: str, share_token: str, parent_file_id='root'):
        url = 'https://api.aliyundrive.com/adrive/v2/file/list_by_share'
        json_data = {
            'share_id': re.search(r'https?://www\.aliyundrive\.com/s/(.*)\??', share_url).group(1),
            'parent_file_id': parent_file_id,
            'order_by': 'name',
            'order_direction': 'DESC'}

        async with self._s.post(url, json=json_data, headers={'x-share-token': share_token}) as resp:
            txt = await resp.text()
            if not HttpResponseErrorHandler.success(resp.status):
                raise HttpResponseErrorHandler.id(resp.status, txt).exception
            js = await resp.json()
            return js['items']

    async def _batch_transfer_internal(self,
                              share_tokens: Iterable[str],
                              file_ids: Iterable[str],
                              share_ids: Iterable[str],
                              to_drive_ids:Iterable[str],
                              to_parent_file_ids: Iterable[str] = itertools.repeat('root'),
                              auto_renames:Iterable[bool] = itertools.repeat(False)):
        url = 'https://api.aliyundrive.com/adrive/v2/batch'
        headers = {'authorization': f'Bearer {self._auth_token}'}
        json_data = {
            'requests': [{
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
                'url': '/file/copy',
            } for i, (share_token, file_id, share_id, to_drive_id, to_parent_file_id, auto_rename) in
                enumerate(zip(share_tokens, file_ids, share_ids, to_drive_ids, to_parent_file_ids, auto_renames))],
            'resource': 'file',
        }
        async with self._s.post(url, json=json_data, headers=headers) as resp:
            txt = await resp.text()
            if not HttpResponseErrorHandler.success(resp.status):
                raise HttpResponseErrorHandler.id(resp.status, txt).exception
            js = await resp.json()
            return js['responses']

    async def _batch_transfer(self, *tasks: TransferTask):
        try:
            token_futures = []
            mkpath_futures = []
            for task in tasks:
                task.start()
                token_futures.append(self._loop.create_task(self._get_share_token(task.url, task.code)))
                mkpath_futures.append(self._loop.create_task(self._dir_driver.mkpath(task.target_dir)))
                await asyncio.sleep(0)

            info_futures = []
            for task, token_future in zip(tasks, token_futures):
                token = await token_future
                info_futures.append(self._loop.create_task(self._get_share_info(task.url, token)))
                await asyncio.sleep(0)

            transfer_params = []
            for task, mkpath_future, token_future, info_future in zip(tasks, mkpath_futures, token_futures, info_futures):
                mkpath = await mkpath_future
                token = await token_future
                info = await info_future
                if not mkpath:
                    warnings.warn(f'创建文件夹{task.target_dir}出错')

                for info_item in info:
                    try:
                        transfer_param = (token,
                                          info_item['file_id'],
                                          info_item['share_id'],
                                          self._dir_driver.drive_id,
                                          mkpath.cloud_id,
                                          True if TransferFlag.rename in task.flags else False)
                    except AttributeError:
                        transfer_param = (None, )*5
                    transfer_params.append(transfer_param)

            result = await self._batch_transfer_internal(*zip(*transfer_params))
            for task, resp in zip(tasks, result):
                if HttpResponseErrorHandler.success(resp['status']):
                    task.finish(True)
                else:
                    warnings.warn(f'任务{task}转存失败：{resp}')
                    task.finish(False)
        except Exception as e:
            warnings.warn(str(e))
            for task in tasks:
                task.finish(False)
            result = []

        return result

    async def transfer(self, *tasks:AliTransferTask):
        async_task = self._loop.create_task(self._batch_transfer(*tasks))
        return [async_task]

    async def close(self):
        if not self._s.closed:
            await self._s.close()
