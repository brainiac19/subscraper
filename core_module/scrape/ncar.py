import asyncio
import itertools
import re
from collections import defaultdict
from enum import Flag

from bs4 import BeautifulSoup as bs

from core_module.base.task import ScrapeTarget, TransferFlag, ScrapeTask
from core_module.base.driver import ScrapeDriver
from core_module.transfer.baidu import BaiduTransferDriver
from core_module.transfer.aliyun import AliTransferDriver
from utils.bs4_util import text_between
from core_module.modules_manager import ModulesManager


class NcarCaptions:
    raw = re.compile('[\s\S]*生肉[\s\S]*')
    chinese = re.compile('[\s\S]*熟肉[\s\S]*')


class NcarClouds:
    baidu = BaiduTransferDriver.regex
    ali = AliTransferDriver.regex


class NcarTarget(ScrapeTarget):
    def __init__(self, caption_type: re.Pattern, cloud_type: re.Pattern, target_dir='/',
                 flags: Flag = TransferFlag.create_parents):
        self.caption_type = caption_type
        self.cloud_type = cloud_type
        self.target_dir = target_dir
        self.flags = flags


class NcarScrapeTask(ScrapeTask):
    def __init__(self, url, *args: NcarTarget):
        super().__init__(url, *args)


class NcarScrapeDriver(ScrapeDriver):
    regex = re.compile(r'(https?://mcar\.vip.*?)')
    batch_optimized = False

    def __init__(self):
        super().__init__()
        self._s = self._build_session(headers={
            'authority': 'mcar.vip',
            'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh-TW;q=0.7,zh;q=0.6',
            'dnt': '1',
            'sec-ch-ua': '"Microsoft Edge";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.43',
        })
        self._modman = ModulesManager()

    @staticmethod
    def share_extract(share):
        url = share.attrs['href']
        # text from share to next <br>
        text = text_between(share, share.find_next('br'))
        code = re.search(r'\W(\w{4})\s?$', text)
        return url, code.group(1) if code else None

    async def _scrape_single(self, queue: asyncio.Queue, task: NcarScrapeTask):
        task.start()
        async with self._s.get(task.url) as resp:
            soup = bs(await resp.text(), 'lxml')

            try:
                posts = soup.find('div', id='postlist').find_all('div', id=re.compile(r'post_\d.+'))
            except AttributeError:
                return False

            targets_dict = defaultdict(list)
            for target in task.targets:
                targets_dict[target.caption_type.pattern].append(target)

            for post in posts:
                for targets_same_caption_type in targets_dict.values():
                    try:
                        caption_sections = set(
                            navstr.find_parent('div', class_='showhide') for navstr in
                            post.find_all(string=targets_same_caption_type[0].caption_type))
                        for target in targets_same_caption_type:
                            shares = itertools.chain(
                                *(section.find_all(href=target.cloud_type) for section in caption_sections))
                            for share in shares:
                                url = share.attrs['href']
                                # text from share to next <br>
                                text = text_between(share, share.find_next('br'))
                                code_match = re.search(r'\W(\w{4})\s?$', text)
                                code = code_match.group(1) if code_match else None
                                task = self._modman.create_transfer_task(url, target.target_dir, code, target.flags)
                                await queue.put(task)
                                await asyncio.sleep(0)
                    except AttributeError:
                        continue
        task.finish(True)

    async def scrape(self, queue: asyncio.Queue, *tasks: NcarScrapeTask):
        async_tasks = []
        for task in tasks:
            async_tasks.append(self._loop.create_task(self._scrape_single(queue, task)))
        return async_tasks


    async def close(self):
        await self._s.close()
