import asyncio
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