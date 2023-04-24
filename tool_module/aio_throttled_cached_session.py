"""Core functions for cache configuration"""
import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Optional

from aiohttp.typedefs import StrOrURL
from aiohttp_client_cache.backends import CacheBackend
from aiohttp_client_cache.cache_control import ExpirationTime
from aiohttp_client_cache.cache_keys import create_key
from aiohttp_client_cache.response import AnyResponse, set_response_defaults

from tool_module.aio_throttled_session import ThrottledClientSession


class ThrottledCachedSession(ThrottledClientSession):
    """A mixin class for :py:class:`aiohttp.ClientSession` that adds caching support"""

    def __init__(
        self,
        cache: Optional[CacheBackend] = None,
        *args,
        **kwargs,
    ):
        self.cache = cache or CacheBackend()
        self.aggressive_cache = True
        self._request_mutex = defaultdict(asyncio.Lock)
        super().__init__(*args, **kwargs)

    async def _request(
        self, method: str, str_or_url: StrOrURL, expire_after: ExpirationTime = None, **kwargs
    ) -> AnyResponse:
        """Wrapper around :py:meth:`.SessionClient._request` that adds caching"""
        key = create_key(method, str_or_url, **kwargs)
        if self.aggressive_cache:
            await self._request_mutex[key].acquire()

        # Attempt to fetch cached response
        response, actions = await self.cache.request(
            method, str_or_url, expire_after=expire_after, **kwargs
        )

        # Restore any cached cookies to the session
        if response:
            #print(f'request {str_or_url} cache found')
            self.cookie_jar.update_cookies(response.cookies or {}, response.url)
            for redirect in response.history:
                self.cookie_jar.update_cookies(redirect.cookies or {}, redirect.url)
            if self.aggressive_cache:
                self._request_mutex[key].release()

            return response
        # If the response was missing or expired, send and cache a new request
        else:
            #print(f'request {str_or_url} cache not found')
            new_response = await super()._request(method, str_or_url, **kwargs)  # type: ignore
            actions.update_from_response(new_response)
            if await self.cache.is_cacheable(new_response, actions):
                await self.cache.save_response(new_response, actions.key, actions.expires)
            if self.aggressive_cache:
                self._request_mutex[key].release()
            return set_response_defaults(new_response)

    async def close(self):
        """Close both aiohttp connector and any backend connection(s) on contextmanager exit"""
        await super().close()
        await self.cache.close()

    @asynccontextmanager
    async def cache_disabled(self):
        """Temporarily disable the cache

        Example:

            >>> async with ThrottledCachedSession() as session:
            >>>     await session.get('http://httpbin.org/ip')
            >>>     async with session.cache_disabled():
            >>>         # Will return a new response, not a cached one
            >>>         await session.get('http://httpbin.org/ip')
        """
        self.cache.disabled = True
        yield
        self.cache.disabled = False

    @asynccontextmanager
    async def aggressive_disabled(self):
        """Temporarily disable the aggresive cache
        """
        self.aggressive_cache = False
        yield
        self.aggressive_cache = True

    async def delete_expired_responses(self):
        """Remove all expired responses from the cache"""
        await self.cache.delete_expired_responses()


