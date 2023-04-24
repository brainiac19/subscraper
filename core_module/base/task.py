import enum
import time
from enum import Flag, auto
from pathlib import PurePosixPath


class Status:
    CREATED = 2
    STARTED = 1
    SUCCESS = 0
    ERROR = -1


class Task:
    def __init__(self):
        self.creation_time = time.time()
        self.status = Status.CREATED
        self.start_time = None
        self.finish_time = None
        self.task_name = None

    def start(self):
        if self.started:
            return
        self.start_time = time.time()
        self.status = Status.STARTED

    def finish(self, success):
        if self.finished:
            return
        self.finish_time = time.time()
        self.status = Status.SUCCESS if success else Status.ERROR

    def set_name(self, name):
        self.task_name = name

    @property
    def status_str(self):
        for name, val in vars(Status).items():
            if self.status == val:
                return name
        return 'UNKNOWN'


    @property
    def started(self):
        return self.status != Status.CREATED

    @property
    def finished(self):
        return self.status == Status.SUCCESS or self.status == Status.ERROR

    @property
    def successful(self):
        return self.status == Status.SUCCESS

    @property
    def error(self):
        return self.status == Status.ERROR


def tasks_completed(*tasks):
    if all(task.finished for task in tasks):
        return True
    return False


class TransferFlag(Flag):
    create_parents = auto()
    rename = auto()
    force = auto()


class TransferTask(Task):
    def __init__(self, url: str, target_dir: str | PurePosixPath = PurePosixPath('/'), code: int | str = None,
                 flags: enum.Flag = Flag(0)):
        super().__init__()
        self.url = url
        self.target_dir = target_dir if isinstance(target_dir, PurePosixPath) else PurePosixPath(target_dir)
        self.code = str(code) if code else None
        self.flags = flags

    def __repr__(self):
        return self.url

    def __hash__(self):
        return hash(f"{self.url}.{self.code}.{self.target_dir.as_posix()}.{self.flags}")


class ScrapeTarget:
    ...


class ScrapeTask(Task):
    def __init__(self, url: str, *args: ScrapeTarget):
        super().__init__()
        self.url = url
        self.targets = args
