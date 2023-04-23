import random
import secrets
from pathlib import PurePath
from typing import Iterable

from ordered_set import OrderedSet


def ordset_rm(orig:OrderedSet, rm:Iterable):
    for item in rm:
        orig.remove(item)
    return orig


def rand_cap(s:str):
    result = ''
    for c in s:
        if bool(random.getrandbits(1)):
            result += c.capitalize()
        else:
            result += c
    return result


def rand_b64(length: int):
    return secrets.token_urlsafe(length)


def dir_level(path:PurePath):
    return len(path.parts) - 1

