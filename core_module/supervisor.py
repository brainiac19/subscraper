import asyncio
import itertools
import os
import pickle
import re
import warnings
from pathlib import Path

warnings.simplefilter('always', UserWarning)
from core_module.modules_manager import ModulesManager
from core_module.base.driver import ScrapeDriver, TransferDriver
from core_module.base.task import ScrapeTask


class TranscrapeSupervisor:
    def __init__(self, *args: ScrapeDriver | TransferDriver):
        self._loop = asyncio.get_event_loop()
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
        self._transfer_listener_coro = None
        self._start_listener()
        self._all_waiter_coro = None
        self._status_updater_coro = None

    @property
    def persistent_data(self):
        data = []
        for driver in self.drivers:
            data.append((type(driver), driver.persistent_data))
        return data

    @persistent_data.setter
    def persistent_data(self, value):
        driver_li = list(self.drivers)
        # if not len(driver_li) == len(value):
        #     return
        for driver_type, data in value:
            for driver in driver_li:
                if isinstance(driver, driver_type):
                    driver.persistent_data = data
                    driver_li.remove(driver)

    def persistent_dump(self, parent_path):
        parent_path = parent_path if isinstance(parent_path, Path) else Path(parent_path)
        parent_path.mkdir(parents=True, exist_ok=True)
        for i, (driver, data) in enumerate(zip(self.drivers, self.persistent_data)):
            type_name = re.search(r"'(.*?)'", str(type(driver))).group(1)
            with open(str(parent_path.joinpath(f'{type_name}-{i}.dpd')), 'wb') as f:
                pickle.dump(data, f)


    def persistent_load(self, parent_path):
        try:
            parent_path = parent_path if isinstance(parent_path, Path) else Path(parent_path)
            data_list = []
            for file in os.scandir(str(parent_path)):
                if not file.name.endswith('.dpd'):
                    continue
                with open(str(parent_path.joinpath(file.name)), 'rb') as f:
                    data = pickle.load(f)
                    data_list.append(data)

            self.persistent_data = data_list
            return True
        except FileNotFoundError:
            return False

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
        while True:
            try:
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

    def _start_listener(self):
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
        for as_task in self.async_tasks:
            as_task.cancel()
        for driver in self.drivers:
            close_coro = getattr(driver, 'close')
            if close_coro and asyncio.iscoroutinefunction(close_coro):
                await close_coro()
        if self._all_waiter_coro:
            self._all_waiter_coro.cancel()

        if self._transfer_listener_coro:
            self._transfer_listener_coro.cancel()

        self.stop_update_status()


    async def start_all_scrape_tasks(self):
        for task_li, driver, async_li in zip(self.pending_scrape_tasks_each_driver,
                                             self.scrape_drivers,
                                             self.async_scrape_tasks_each_driver):
            if task_li:
                async_li.extend(await driver.scrape(self._transfer_task_q, *task_li))

    async def wait_all_scrape_tasks(self):
        try:
            await asyncio.wait(self.async_scrape_tasks)
        except (ValueError, AssertionError):
            pass

    async def wait_transfer_task_queue_empty(self):
        await self._transfer_task_q.join()

    async def start_all_transfer_tasks(self):
        for task_li, driver, async_li in zip(self.pending_transfer_tasks_each_driver,
                                             self.transfer_drivers,
                                             self.async_transfer_tasks_each_driver):
            if task_li:
                async_li.extend(await driver.transfer(*task_li))

    async def wait_all_transfer_tasks(self):
        try:
            await asyncio.wait(self.async_transfer_tasks)
        except (ValueError, AssertionError):
            pass

    async def start_and_await_all(self):
        await self.start_all_scrape_tasks()
        await self.wait_all_scrape_tasks()
        await self.wait_transfer_task_queue_empty()
        await self.start_all_transfer_tasks()
        await self.wait_all_transfer_tasks()

    def start_all(self):
        self._all_waiter_coro = self._loop.create_task(self.start_and_await_all())

    def await_all(self):
        if not self._all_waiter_coro:
            self.start_all()
        self._loop.run_until_complete(asyncio.wait_for(self._all_waiter_coro, timeout=None))

    @property
    def status_str(self):
        string = f'分享链接获取任务：\n\t共{len(list(self.scrape_tasks))}个，' \
                 f'成功{len([task for task in self.scrape_tasks if task.successful])}个，' \
                 f'错误{len([task for task in self.scrape_tasks if task.error])}个' \
                 f'网盘转存任务：\n\t共{len(list(self.transfer_tasks))}个，' \
                 f'成功{len([task for task in self.transfer_tasks if task.successful])}个，' \
                 f'错误{len([task for task in self.transfer_tasks if task.error])}个'
        return string

    def print_status(self):
        print(self.status_str)

    async def async_print_status(self, interval: float = 0.25, last_update:float = 1):
        while True:
            try:
                self.print_status()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
        if last_update > 0:
            await asyncio.sleep(last_update)
            self.print_status()

    def start_update_status(self, interval:float = 0.25, last_update:float = 1):
        self._status_updater_coro = self._loop.create_task(self.async_print_status(interval, last_update))

    def stop_update_status(self):
        if self._status_updater_coro:
            self._status_updater_coro.cancel()
            self._status_updater_coro = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._loop.run_until_complete(self.stop())
