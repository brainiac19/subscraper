import asyncio
import typing
from abc import ABC, abstractmethod
from collections import defaultdict
from queue import Queue
from typing import Iterable

import aiohttp
from ordered_set import OrderedSet

from core_module.base.task import Task
from utils.common import ordset_rm


class Driver(ABC):
    @property
    @abstractmethod
    def regex(self) -> typing.Pattern:
        ...

    @property
    @abstractmethod
    def batch_optimized(self) -> bool:
        ...

    @property
    def persistent_data(self):
        return None

    @persistent_data.setter
    def persistent_data(self, value):
        pass

    def __init__(self):
        self._loop = asyncio.get_event_loop()

    def _build_session(self, aio_session=aiohttp.ClientSession, *args, **kwargs):
        return self._loop.run_until_complete(self._build_session_async(aio_session, *args, **kwargs))

    async def _build_session_async(self, aio_session=aiohttp.ClientSession, *args, **kwargs):
        return aio_session(*args, **kwargs)


class AuthDriver(Driver):
    ...


class ScrapeDriver(Driver):

    @abstractmethod
    def scrape(self, queue: Queue, *tasks: Task) -> Iterable[asyncio.Task]:
        ...


class DirectoryItem:
    def __init__(self, unique_id, unique_parent_id, name=None):
        self.unique_id = unique_id
        self.unique_parent_id = unique_parent_id
        self.name = name
        if self.is_root and not self.name:
            self.name = 'root'

    @property
    def is_root(self):
        if self.unique_id == self.unique_parent_id:
            return True
        return False

    def __repr__(self):
        return self.name if self.name else self.unique_id

    def __eq__(self, other):
        if not isinstance(other, DirectoryItem):
            return False
        if self.unique_id == other.unique_id and self.unique_parent_id == other.unique_parent_id:
            return True
        else:
            return False

    def __hash__(self):
        return hash(self.unique_id + '.' + self.unique_parent_id)


class DirectoryDriver(Driver, ABC):
    def __init__(self):
        super().__init__()
        self._dir_cache = defaultdict(OrderedSet)

    def cache_add(self, unique_parent_id: str, items: Iterable[DirectoryItem] | DirectoryItem,
                  renew=False):
        if isinstance(items, Iterable):
            if renew:
                self._dir_cache[unique_parent_id] = OrderedSet(items)
            else:
                self._dir_cache[unique_parent_id].update(OrderedSet(items))
        else:
            self._dir_cache[unique_parent_id].append(items)

    def cache_ls(self, unique_parent_id: str) -> OrderedSet[DirectoryItem] | None:
        if unique_parent_id not in self._dir_cache.keys():
            return None
        else:
            return self._dir_cache[unique_parent_id]

    def cache_get(self, unique_id: str, *, unique_parent_id: str = None, cache: OrderedSet = None):
        if not cache:
            cache = self.cache_ls(unique_parent_id)
        if cache is None:
            return None
        for item in cache:
            if item.unique_id == unique_id:
                return item
        return None

    def cache_exists(self, dir_item: DirectoryItem):
        if dir_item.is_root:
            return dir_item
        ordset = self.cache_ls(dir_item.unique_parent_id)
        if not ordset:
            return None
        for elem in ordset:
            if dir_item == elem:
                return elem
        else:
            return False

    def cache_rm(self, unique_parent_id: str, items: Iterable[DirectoryItem] | DirectoryItem):
        if isinstance(items, Iterable):
            ordset_rm(self._dir_cache[unique_parent_id], items)
        else:
            self._dir_cache[unique_parent_id].remove(items)


class TransferDriver(Driver, ABC):

    @abstractmethod
    def transfer(self, *tasks: Task) -> Iterable[asyncio.Task]:
        ...


class ResponseError(Exception):
    def __init__(self, id: int | typing.Any, description: str):
        self.id = id
        self.description = description
        super().__init__(self.description)


class ResponseCode:
    def __init__(self, id: int | typing.Any, description: str):
        self.id = id
        self.description = description

    @property
    def exception(self):
        return ResponseError(self.id, self.description)


class ErrorHandler(ABC):
    UNKNOWN_ERROR = ResponseCode(None, 'Unknown error')

    @classmethod
    def id(cls, errno, errmsg=''):
        if errno is None:
            if errmsg:
                return ResponseCode(errno, errmsg)
            else:
                return cls.UNKNOWN_ERROR
        for k, v in dict(vars(cls)).items():
            if isinstance(v, ResponseCode) and errno == v.id:
                return v
        return ResponseCode(errno, errmsg)

    @abstractmethod
    def success(self, errno):
        ...


class HttpResponseErrorHandler(ErrorHandler):
    SUCCESS = ResponseCode(200, 'Request success')
    POST_SUCCESS = ResponseCode(201, 'POST success')
    Accepted = ResponseCode(202, 'Accepted')

    @classmethod
    def success(cls, errno):
        if not isinstance(errno, int):
            errno = int(errno)
        if errno == cls.SUCCESS.id or errno == cls.POST_SUCCESS.id or errno == cls.Accepted.id:
            return True
        else:
            return False
