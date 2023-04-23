import enum
import importlib
import inspect
import itertools
import os
from pathlib import Path

from core_module.base.driver import Driver, TransferDriver, ScrapeDriver
from core_module.base.task import Task, TransferTask, ScrapeTask


class ModulesManager:
    def __init__(self):
        self._mods = self.load_modules()

    def load_modules(self):
        mods = []
        dirs = ['core_module/transfer', 'core_module/scrape']
        for file in itertools.chain(*[os.scandir(_dir) for _dir in dirs]):
            if not file.name.endswith('.py'):
                continue
            path = Path(file.path)
            mod_name = path.as_posix()[:-3].replace('/', '.')
            mod = importlib.import_module(mod_name)
            mods.append(mod)
        return mods

    def mod_get_subcls(self, mod, cls_type):
        cls_ls = []
        for name, cls in inspect.getmembers(mod, lambda member: inspect.isclass(member) and member.__module__ == mod.__name__):
            if issubclass(cls, cls_type):
                cls_ls.append(cls)
        return cls_ls

    def mod_get_drivers(self, mod):
        return self.mod_get_subcls(mod, Driver)

    def mod_get_tasks(self,mod):
        return self.mod_get_subcls(mod, Task)

    def mod_get_transfer_driver(self, mod):
        drivers = self.mod_get_subcls(mod, TransferDriver)
        if drivers:
            return drivers[0]
        return None

    def mod_get_scrape_driver(self, mod):
        drivers = self.mod_get_subcls(mod, ScrapeDriver)
        if drivers:
            return drivers[0]
        return None

    def mod_get_transfer_task(self, mod):
        drivers = self.mod_get_subcls(mod, TransferTask)
        if drivers:
            return drivers[0]
        return None

    def mod_get_scrape_task(self, mod):
        drivers = self.mod_get_subcls(mod, ScrapeTask)
        if drivers:
            return drivers[0]
        return None

    def task_find_mod(self, task:type(Task)):
        for mod in self._mods:
            get_tasks = self.mod_get_tasks(mod)
            if task in get_tasks:
                return mod

    def task_find_transfer_driver(self, task:type(Task)):
        mod = self.task_find_mod(task)
        return self.mod_get_transfer_driver(mod)

    def task_find_scrape_driver(self, task:type(Task)):
        mod = self.task_find_mod(task)
        return self.mod_get_scrape_driver(mod)

    def url_find_mod(self, url:str):
        for mod in self._mods:
            drivers = self.mod_get_subcls(mod, TransferDriver)
            for driver in drivers:
                if driver.regex.match(url):
                    return mod

    def url_find_transfer_driver(self, url:str):
        mod = self.url_find_mod(url)
        return self.mod_get_transfer_driver(mod)

    def url_find_transfer_task(self, url:str):
        mod = self.url_find_mod(url)
        return self.mod_get_transfer_task(mod)


    def create_transfer_task(self, url: str, target_dir: str = '/', code: str | int = None,
                             flags: enum.Flag = enum.Flag(0),
                             **kwargs):
        task_cls = self.url_find_transfer_task(url)
        params = inspect.signature(task_cls).parameters
        cls_kwargs = {'url': url, 'target_dir': target_dir, 'code': code, 'flags': flags}
        for arg, value in kwargs.items():
            if arg in params and arg not in cls_kwargs.keys():
                cls_kwargs[arg] = value
        return task_cls(**cls_kwargs)