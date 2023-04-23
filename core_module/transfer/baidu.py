import asyncio
import json
import re
import typing
import warnings

warnings.simplefilter('always', UserWarning)
from collections import defaultdict
from enum import Flag
from pathlib import PurePosixPath
from urllib.parse import urlparse

import aiohttp
from aiohttp_client_cache import CacheBackend
from ordered_set import OrderedSet

from core_module.base.driver import DirectoryItem, DirectoryDriver, HttpResponseErrorHandler, TransferDriver, \
    ErrorHandler, ResponseCode, ResponseError, AuthDriver
from core_module.base.task import TransferTask, TransferFlag
from tool_module.aio_throttled_cached_session import ThrottledCachedSession
from utils.async_io import class_decorator
from utils.http_util import ck_str_to_dict


class BaiduAuthErrorHandler(ErrorHandler):
    SUCCESS = ResponseCode(0, '成功')
    INVALID_URL = ResponseCode(105, '无效的URL')
    TOO_MANY_FAILS = ResponseCode(-62, '请求过于频繁，请稍后再试')
    INVALID_LOGIN = ResponseCode(-4, '无效的登录，请注销其它终端的登录')

    @classmethod
    def success(cls, errno: int | typing.Any):
        if errno == cls.SUCCESS.id:
            return True
        else:
            return False


class BaiduAuthDriver(AuthDriver):
    regex = re.compile(r'https?://pan\.baidu\.com.*?')
    batch_optimized = False

    def __init__(self, cookies: str | dict = None, bds_token: str = None, aio_session: aiohttp.ClientSession = None):
        super().__init__()
        if type(cookies) is str:
            cookies = ck_str_to_dict(cookies)
        if aio_session:
            self.s = aio_session
            self.s.cookie_jar.update_cookies(cookies)
        else:
            self.s = self._build_session(
                aio_session=ThrottledCachedSession,
                headers={'Host': 'pan.baidu.com',
                         'Connection': 'keep-alive',
                         'Upgrade-Insecure-Requests': '1',
                         'Sec-Fetch-Dest': 'document',
                         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.39',
                         'Sec-Fetch-Site': 'same-site',
                         'Sec-Fetch-Mode': 'navigate',
                         'Referer': 'https://pan.baidu.com',
                         'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7,en-GB;q=0.6,ru;q=0.5'},
                cookie_jar=aiohttp.CookieJar(quote_cookie=False), cookies=cookies,
                cache=CacheBackend(allowed_methods=('GET', 'HEAD', 'POST'), allowed_codes=(200, 201, 202)))
        self.cookies = cookies
        self._bds_token = bds_token

    @property
    def persistent_data(self):
        data = {
            'cookies': self.cookies
        }
        return data

    @persistent_data.setter
    def persistent_data(self, value):
        self.cookies = value['cookies']
        self.s.cookie_jar.update_cookies(value['cookies'])

    @staticmethod
    def require_bds_token(func):
        def ensure_req(self):
            if self._bds_token is None:
                self._bds_token = self._loop.run_until_complete(self._get_bdstoken())

        return class_decorator(ensure_req, func)

    async def _get_bdstoken(self):
        url = 'https://pan.baidu.com/api/gettemplatevariable'
        params = {
            'clienttype': '0',
            'app_id': '250528',
            'web': '1',
            'fields': '["bdstoken", "token", "uk", "isdocuser", "servertime"]'
        }
        async with self.s.get(url, params=params) as resp:
            txt = await resp.text()
            if not HttpResponseErrorHandler.success(resp.status):
                raise HttpResponseErrorHandler.id(resp.status, txt).exception
            js = await resp.json()
            if BaiduAuthErrorHandler.success(js['errno']):
                return js['result']['bdstoken']
            else:
                raise BaiduAuthErrorHandler.id(js['errno']).exception

    @property
    @require_bds_token
    def bds_token(self):
        return self._bds_token

    async def close(self):
        if not self.s.closed:
            await self.s.close()


class BaiduDirectoryErrorHandler(ErrorHandler):
    SUCCESS = ResponseCode(0, '成功')
    INVALID_URL = ResponseCode(105, '无效的URL')
    DIRECTORY_DNE = ResponseCode(-9, '请求的目录不存在')
    TOO_MANY_FAILS = ResponseCode(-62, '请求过于频繁，请稍后再试')
    INVALID_LOGIN = ResponseCode(-4, '无效的登录，请注销其它终端的登录')

    @classmethod
    def success(cls, errno: int | typing.Any):
        if errno == cls.SUCCESS.id:
            return True
        else:
            return False


class BaiduDirectoryItem(DirectoryItem):
    def __init__(self, path: str | PurePosixPath, is_dir: bool):
        self.path = path if isinstance(path, PurePosixPath) else PurePosixPath(path)
        self.is_dir = is_dir
        super().__init__(self.calc_id(self.path, is_dir),
                         self.calc_id(self.path.parent, True),
                         self.path.name)

    @staticmethod
    def calc_id(path: PurePosixPath | str, is_dir: bool):
        path = path if isinstance(path, PurePosixPath) else PurePosixPath(path)
        return path.as_posix() + '//' + str(is_dir)


class BaiduDirectoryDriver(DirectoryDriver):
    regex = re.compile(r'https?://pan\.baidu\.com.*?')
    batch_optimized = False

    def __init__(self, *, cookies: str | dict = None, bds_token: str = None, aio_session: aiohttp.ClientSession = None,
                 auth_driver: BaiduAuthDriver = None):
        super().__init__()
        if auth_driver:
            self.auth_driver = auth_driver

        else:
            self.auth_driver = BaiduAuthDriver(cookies, bds_token, aio_session)
        self.s = self.auth_driver.s
        self.s.set_host_rate_limit('pan.baidu.com', 10)
        self._mkpath_mutex = asyncio.Lock()

    async def ls(self, path: PurePosixPath | str = PurePosixPath('/'), renew=False) -> dict | OrderedSet[
        BaiduDirectoryItem] | typing.Any:
        path = path if isinstance(path, PurePosixPath) else PurePosixPath(path)
        url = 'https://pan.baidu.com/api/list'
        params = {
            'order': 'time',
            'desc': '1',
            'showempty': '0',
            'web': '1',
            'page': '1',
            'num': '1000',
            'dir': path.as_posix(),
            'bdstoken': self.auth_driver.bds_token
        }
        async with self.s.cache_disabled():
            async with self.s.get(url, params=params) as resp:
                txt = await resp.text()
                if not HttpResponseErrorHandler.success(resp.status):
                    raise HttpResponseErrorHandler.id(resp.status, txt).exception
                js = await resp.json()
                if BaiduDirectoryErrorHandler.success(js['errno']):
                    dir_items = OrderedSet(
                        BaiduDirectoryItem(path.joinpath(item['server_filename']).as_posix(),
                                           True if item['isdir'] == 1 else False) for item in js['list'])
                    self.cache_add(BaiduDirectoryItem.calc_id(path, True), dir_items, renew)
                    return dir_items
                elif BaiduDirectoryErrorHandler.id(js['errno']) == BaiduDirectoryErrorHandler.DIRECTORY_DNE:
                    return None
                else:
                    raise BaiduDirectoryErrorHandler.id(js['errno']).exception

    async def exists(self, dir_item: BaiduDirectoryItem):
        cached_exists = self.cache_exists(dir_item)
        if cached_exists is not None:
            return cached_exists
        await self.ls(dir_item.path.parent)
        return self.cache_exists(dir_item)

    async def mkpath(self, path: PurePosixPath | str):
        await self._mkpath_mutex.acquire()
        path = path if isinstance(path, PurePosixPath) else PurePosixPath(path)
        dir_item = BaiduDirectoryItem(path, True)
        if await self.exists(dir_item):
            self._mkpath_mutex.release()
            return True

        url = 'https://pan.baidu.com/api/create'
        params = {
            'a': 'commit',
            'bdstoken': self.auth_driver.bds_token
        }
        data = {
            'path': path.as_posix(),
            'isdir': '1',
            'block_list': '[]'
        }
        async with self.s.post(url, params=params, data=data) as resp:
            txt = await resp.text()
            if not HttpResponseErrorHandler.success(resp.status):
                raise HttpResponseErrorHandler.id(resp.status, txt).exception
            js = await resp.json()
            if BaiduDirectoryErrorHandler.success(js['errno']):
                self.cache_add(BaiduDirectoryItem.calc_id(path.parent, True), dir_item)
                self._mkpath_mutex.release()
                return True
            else:
                self._mkpath_mutex.release()
                raise BaiduDirectoryErrorHandler.id(js['errno']).exception

    async def close(self):
        if not self.s.closed:
            await self.s.close()


class BaiduTransferErrorHandler(ErrorHandler):
    SUCCESS = ResponseCode(0, '成功')
    INVALID_URL = ResponseCode(105, '无效的URL')
    WRONG_CODE = ResponseCode(-9, '提取码错误')
    TOO_MANY_FAILS = ResponseCode(-62, '请求过于频繁，请稍后再试')
    INVALID_LOGIN = ResponseCode(-4, '无效的登录，请注销其它终端的登录')
    FILE_EXISTS = ResponseCode(4, '已存在同名文件')
    DIR_EXISTS = ResponseCode(-8, '已存在同名文件夹')
    TOO_MANY_FILES = ResponseCode(12, '转存文件数超过限制')

    @classmethod
    def success(cls, errno: int | typing.Any):
        if errno == cls.SUCCESS.id:
            return True
        else:
            return False


class BaiduTransferTask(TransferTask):
    def __init__(self, url: str, target_dir: str | PurePosixPath = PurePosixPath('/'), code: int | str = None,
                 flags: Flag = TransferFlag.create_parents):
        url, url_code = self.pre_proc(url)
        super().__init__(url, target_dir, code if code else url_code, flags)

    @staticmethod
    def pre_proc(raw_url: str):
        parsed = urlparse(raw_url)
        url = f"{parsed.scheme}://{parsed.netloc + parsed.path}"
        code = None
        try:
            code = re.search(r'pwd=(.*)', parsed.query).group(1)
        except AttributeError:
            pass
        return url, code


class BaiduTransferDriver(TransferDriver):
    regex = re.compile(r'https?://pan\.baidu\.com.*?')
    batch_optimized = False

    def __init__(self, cookies: str | dict = None, bds_token: str = None, aio_session: aiohttp.ClientSession = None,
                 auth_driver: BaiduAuthDriver = None):
        super().__init__()
        if auth_driver:
            self.auth_driver = auth_driver

        else:
            self.auth_driver = BaiduAuthDriver(cookies, bds_token, aio_session)
        self.s = self.auth_driver.s
        self.s.set_host_rate_limit('pan.baidu.com', 10)
        self.dir_driver = BaiduDirectoryDriver(auth_driver=self.auth_driver)
        self._task_mutex = defaultdict(asyncio.Lock)
        self._bdclnd_mutex = defaultdict(asyncio.Lock)
        self._bdclnd_cache = {}

    @property
    def persistent_data(self):
        data = {
            'auth_driver_persistent_data': self.auth_driver.persistent_data,
            'bdclnd_cache': self._bdclnd_cache
        }
        return data

    @persistent_data.setter
    def persistent_data(self, value):
        self._bdclnd_cache = value['bdclnd_cache']
        self.auth_driver.persistent_data = value['auth_driver_persistent_data']

    async def _get_bdstoken(self):
        url = 'https://pan.baidu.com/api/gettemplatevariable'
        params = {
            'clienttype': '0',
            'app_id': '250528',
            'web': '1',
            'fields': '["bdstoken", "token", "uk", "isdocuser", "servertime"]'
        }
        async with self.s.get(url, params=params) as resp:
            txt = await resp.text()
            if not HttpResponseErrorHandler.success(resp.status):
                raise HttpResponseErrorHandler.id(resp.status, txt).exception
            js = await resp.json()
            if BaiduTransferErrorHandler.success(js['errno']):
                return js['result']['bdstoken']
            else:
                raise BaiduTransferErrorHandler.id(js['errno']).exception

    async def _get_bdclnd(self, share_url: str, code: str = None):
        await self._bdclnd_mutex[share_url].acquire()
        verify_url = 'https://pan.baidu.com/share/verify'
        params = {
            'surl': re.search(r'https?://pan\.baidu\.com/s/1(.*)\??', share_url).group(1),
            'bdstoken': self.auth_driver.bds_token,
            'channel': 'chunlei',
            'web': '1',
            'clienttype': '0'
        }
        data = {
            'pwd': code,
            'vcode': '',
            'vcode_str': ''
        }
        async with self.s.post(verify_url, params=params, data=data) as resp:
            txt = await resp.text()
            if not HttpResponseErrorHandler.success(resp.status):
                raise HttpResponseErrorHandler.id(resp.status, txt).exception
            js = await resp.json()
            self._bdclnd_mutex[share_url].release()
            if BaiduTransferErrorHandler.success(js['errno']):
                self._bdclnd_cache[share_url] = js['randsk']
                return js['randsk']
            else:
                raise BaiduTransferErrorHandler.id(js['errno']).exception

    async def _get_share_info(self, share_url: str, bdclnd: str = None):
        async with self.s.get(
                share_url,
                cookies={'BDCLND': bdclnd} if bdclnd else None,
                headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh-TW;q=0.7,zh;q=0.6',
                    'Connection': 'keep-alive',
                    'DNT': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.39',
                    'sec-ch-ua': '"Chromium";v="112", "Microsoft Edge";v="112", "Not:A-Brand";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                }) as resp:
            txt = await resp.text()
            if not HttpResponseErrorHandler.success(resp.status):
                raise HttpResponseErrorHandler.id(resp.status, txt).exception
            try:
                content = await resp.text()
                match = re.search(r"locals\.mset\((.*?)\);", content).group(1)
                return json.loads(match)
            except (UnicodeDecodeError, AttributeError, json.JSONDecodeError) as e:
                raise ResponseError(None, "Failed to get files")

    async def _transfer_single_internal(self, target_dir, share_id, share_uk, fs_id_list, bdclnd=None):
        url = 'https://pan.baidu.com/share/transfer'
        params = {
            'shareid': share_id,
            'from': share_uk,
            'bdstoken': self.auth_driver.bds_token,
            'channel': 'chunlei',
            'web': '1',
            'clienttype': '0'
        }
        data = {
            'fsidlist': f'[{",".join(str(s) for s in fs_id_list)}]',
            'path': target_dir
        }
        async with self.s.post(url, params=params, data=data, cookies={'BDCLND': bdclnd} if bdclnd else None) as resp:
            txt = await resp.text()
            if not HttpResponseErrorHandler.success(resp.status):
                raise HttpResponseErrorHandler.id(resp.status, txt).exception
            js = await resp.json()
            if BaiduTransferErrorHandler.success(js['errno']):
                return True
            else:
                raise BaiduTransferErrorHandler.id(js['errno']).exception

    async def _transfer_single(self, task: BaiduTransferTask):
        task.start()
        await self._task_mutex[task].acquire()
        try:
            bdclnd = (await self._get_bdclnd(task.url, task.code)) if task.code else None
            share_info = await self._get_share_info(task.url, bdclnd)

            share_id = share_info['shareid']
            share_uk = share_info['share_uk']
            share_file_list = share_info['file_list']
            if len(share_file_list) == 0:
                task.finish(False)
                self._task_mutex[task].release()
                return task
            share_name = share_file_list[0]['server_filename'] if len(share_file_list) == 1 \
                else share_file_list[0]['server_filename'] + ' 等文件'
            task.set_name(share_name)

            fs_list = []
            future_dir_items = []
            for file in share_file_list:
                future_dir_item = BaiduDirectoryItem(task.target_dir.joinpath(file['server_filename']),
                                                     True if file['isdir'] == 1 else False)
                if TransferFlag.force in task.flags or (not await self.dir_driver.exists(future_dir_item)):
                    fs_list.append(file['fs_id'])
                    future_dir_items.append(future_dir_item)

            if len(fs_list) == 0:
                task.finish(True)
                self._task_mutex[task].release()
                return task

            try:
                if TransferFlag.create_parents in task.flags:
                    await self.dir_driver.mkpath(task.target_dir)
                result = await self._transfer_single_internal(task.target_dir, share_id, share_uk, fs_list, bdclnd)
            except ResponseError as e:
                raise e
            if result:
                self.dir_driver.cache_add(task.target_dir.as_posix(), future_dir_items)
            task.finish(result)
        except ResponseError as e:
            warnings.warn(f"{task.url + f'?pwd={task.code}' if task.code else ''} 转存失败：{e.description}")
            task.finish(False)
        self._task_mutex[task].release()
        return task

    async def transfer(self, *tasks: BaiduTransferTask):
        async_tasks = []
        for task in tasks:
            async_tasks.append(self._loop.create_task(self._transfer_single(task)))
        return async_tasks

    async def close(self):
        if not self.s.closed:
            await self.s.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._loop.run_until_complete(self.close())
