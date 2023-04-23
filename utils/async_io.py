import asyncio
import functools
import os
import nest_asyncio


def init_loop():
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    loop = asyncio.new_event_loop()
    nest_asyncio.apply(loop)
    asyncio.set_event_loop(loop)


def async_run(*args, **kwargs):
    init_loop()
    asyncio.run(*args, **kwargs)


def class_decorator(wrapping_logic, orig_func):
    @functools.wraps(orig_func)
    def wrapper(self, *args, **kwargs):
        wrapping_logic(self, *args, **kwargs)
        if not asyncio.iscoroutinefunction(orig_func):
            return orig_func(self, *args, **kwargs)
        else:
            async def tmp():
                return await orig_func(self, *args, **kwargs)
            return tmp()
    return wrapper
