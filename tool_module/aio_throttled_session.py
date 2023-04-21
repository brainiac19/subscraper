## -----------------------------------------------------------
#### Class ThrottledClientSession(aiohttp.ClientSession)
#
#  Rate-limited async http client session
#
#  Inherits aiohttp.ClientSession
## -----------------------------------------------------------
import asyncio
import typing
from collections import defaultdict
from typing import Optional, Union
from aiohttp import ClientSession, ClientResponse
from asyncio import Queue, Task, CancelledError, TimeoutError, sleep, create_task, wait_for
from urllib.parse import urlparse
import time
import logging
import re
from math import ceil, log

from aiohttp.typedefs import StrOrURL
from aiohttp_client_cache import get_valid_kwargs


class ThrottleTimeout(TimeoutError):
    def __init__(self, *args):
        super().__init__(*args)


class RateLimit:
    def __init__(self,
                 rate_limit: float = 0):
        self.rate_limit = rate_limit
        self._start_time = None
        self._req_counter = 0
        self._mutex = asyncio.Lock()

    @property
    def rate(self):
        if self._start_time is None:
            return 0
        else:
            return self._req_counter / (time.time() - self._start_time)

    def reset(self):
        self._start_time = None
        self._req_counter = 0

    def time_to_wait(self):
        if self.rate_limit == 0 or self._start_time is None:
            return 0
        else:
            return (self._req_counter + 1) / self.rate_limit + self._start_time - time.time()

    async def ready(self):
        await self._mutex.acquire()

        wait = self.time_to_wait()
        if wait > 0:
            await asyncio.sleep(wait)
        if self._start_time is None:
            self._start_time = time.time()
        self._req_counter += 1
        self._mutex.release()
        return True


class DomainRateLimit(RateLimit):
    def __init__(self, *, domain_re: str, rate_limit: float = 0):
        super().__init__(rate_limit)
        self.domain_re = re.compile(domain_re)


class ThrottledClientSession(ClientSession):
    def __init__(self,
                 base_url: Optional[StrOrURL] = None,
                 *,
                 rate_limit: float = 0,
                 **kwargs
                 ):
        super().__init__(base_url=base_url, **kwargs)
        self.global_rate_limit = RateLimit(rate_limit)
        self.host_rate_limit = defaultdict(RateLimit)
        self.regex_rate_limit = defaultdict(RateLimit)

    def set_host_rate_limit(self, host: str, limit: float):
        self.host_rate_limit[host].rate_limit = limit
        self.host_rate_limit[host].reset()

    def set_regex_rate_limit(self, regex: str | re.Pattern, limit: float):
        if not isinstance(regex, re.Pattern):
            regex = re.compile(regex)
        self.regex_rate_limit[regex].rate_limit = limit
        self.regex_rate_limit[regex].reset()

    async def all_regex_ready(self, url):
        for regex, rate_limit in self.regex_rate_limit.items():
            if regex.match(url):
                await rate_limit.ready()

    async def _request(
            self,
            method: str,
            str_or_url: StrOrURL,
            *args,
            **kwargs) -> ClientResponse:

        host = urlparse(str_or_url).hostname
        await self.host_rate_limit[host].ready()

        await self.all_regex_ready(str_or_url)

        await self.global_rate_limit.ready()
        resp = await super()._request(method, str_or_url, **kwargs)
        return resp
