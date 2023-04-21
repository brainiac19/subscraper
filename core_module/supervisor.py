import asyncio
import inspect
import itertools
import warnings

from core_module.modules_manager import ModulesManager
from core_module.base.driver import Driver, ScrapeDriver, TransferDriver
from core_module.base.task import ScrapeTask


class TranscrapeSupervisor:
    def __init__(self, *args: Driver):
        self._loop = asyncio.get_event_loop()
        self._transfer_listener_coro = None
        self._transfer_task_q = asyncio.Queue()
        self.scrape_drivers = [driver for driver in args if isinstance(driver, ScrapeDriver)]
        self.transfer_drivers = [driver for driver in args if isinstance(driver, TransferDriver)]
        self.scrape_tasks_each_driver = [list() for _ in range(len(self.scrape_drivers))]
        self.pending_scrape_tasks_each_driver = [list() for _ in range(len(self.scrape_drivers))]
        self.async_scrape_tasks_each_driver = [list() for _ in range(len(self.scrape_drivers))]
        self.transfer_tasks_each_driver = [list() for _ in range(len(self.transfer_drivers))]
        self.pending_transfer_tasks_each_driver = [list() for _ in range(len(self.transfer_drivers))]
        self.async_transfer_tasks_each_driver = [list() for _ in range(len(self.transfer_drivers))]
        self._modman = ModulesManager()
        self.start()

    @property
    def drivers(self):
        return itertools.chain(self.scrape_drivers, self.transfer_drivers)

    @property
    def scrape_tasks(self):
        return itertools.chain(*self.scrape_tasks_each_driver)

    @property
    def async_scrape_tasks(self):
        return itertools.chain(*self.async_scrape_tasks_each_driver)

    @property
    def transfer_tasks(self):
        return itertools.chain(*self.transfer_tasks_each_driver)

    @property
    def async_transfer_tasks(self):
        return itertools.chain(*self.async_transfer_tasks_each_driver)

    @property
    def async_tasks(self):
        return itertools.chain(self.async_scrape_tasks, self.async_transfer_tasks)

    def clear_scrape_task(self):
        for li in self.scrape_tasks_each_driver:
            li.clear()

    def clear_transfer_task(self):
        for li in self.transfer_tasks_each_driver:
            li.clear()

    def clear_pending_scrape_task(self):
        for li in self.pending_scrape_tasks_each_driver:
            li.clear()

    def clear_pending_transfer_task(self):
        for li in self.pending_transfer_tasks_each_driver:
            li.clear()

    async def _transfer_listener(self):
        try:
            while True:
                task = await self._transfer_task_q.get()
                self._transfer_task_q.task_done()
                driver_type = self._modman.task_find_transfer_driver(type(task))
                for i, driver in enumerate(self.transfer_drivers):
                    if isinstance(driver, driver_type):
                        self.transfer_tasks_each_driver[i].append(task)
                        if not driver.batch_optimized:
                            async_tasks = await driver.transfer(task)
                            self.async_transfer_tasks_each_driver[i].extend(async_tasks)
                        else:
                            self.pending_transfer_tasks_each_driver[i].append(task)
                        break
                else:
                    warnings.warn(f'A task of type {type(task)} failed to find its\' driver')
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            return True

    def start(self):
        self._transfer_listener_coro = self._loop.create_task(self._transfer_listener())

    async def _submit(self, *tasks: ScrapeTask):
        for task in tasks:
            driver_type = self._modman.task_find_scrape_driver(type(task))
            for i, driver in enumerate(self.scrape_drivers):
                if isinstance(driver, driver_type):
                    self.scrape_tasks_each_driver[i].append(task)
                    if not driver.batch_optimized:
                        async_tasks = await driver.scrape(self._transfer_task_q, task)
                        self.async_scrape_tasks_each_driver[i].extend(async_tasks)
                    else:
                        self.pending_scrape_tasks_each_driver[i].append(task)
                    break
            else:
                warnings.warn(f'A task of type {type(task)} failed to find its\' driver')

    def submit(self, *tasks: ScrapeTask):
        self._loop.run_until_complete(self._submit(*tasks))

    async def stop(self):
        self._transfer_listener_coro.cancel()
        for as_task in self.async_tasks:
            as_task.cancel()
        for driver in self.drivers:
            close_coro = getattr(driver, 'close')
            if close_coro and asyncio.iscoroutinefunction(close_coro):
                await close_coro()

    async def wait_for_all(self):
        for task_li, driver, async_li in zip(self.pending_scrape_tasks_each_driver,
                                             self.scrape_drivers,
                                             self.async_scrape_tasks_each_driver):
            async_li.extend(await driver.scrape(self._transfer_task_q, *task_li))
        try:
            await asyncio.wait(self.async_scrape_tasks)
        except (ValueError, AssertionError):
            pass
        # self.clear_pending_scrape_task()

        await self._transfer_task_q.join()

        for task_li, driver, async_li in zip(self.pending_transfer_tasks_each_driver,
                                             self.transfer_drivers,
                                             self.async_transfer_tasks_each_driver):
            async_li.extend(await driver.transfer(*task_li))
        try:
            await asyncio.wait(self.async_transfer_tasks)
        except (ValueError, AssertionError):
            pass
        # self.clear_pending_transfer_task()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
