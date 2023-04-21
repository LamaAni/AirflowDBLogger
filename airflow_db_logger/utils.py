import os
import sys
import inspect
from typing import Type

global IS_DB_LOGGER_LOADING_CONFIG
IS_DB_LOGGER_LOADING_CONFIG = False


def get_is_no_color():
    val = os.environ.get("NO_COLOR", "--no-color" in sys.argv)
    if not isinstance(val, bool):
        val = val.strip().lower()
        os.environ["NO_COLOR"] = val
    return val


def colorize(val, color, add_reset=True):
    if get_is_no_color():
        return val
    return color + val + ("\033[0m" if add_reset else "")


def get_calling_frame_objects_by_type(otype: Type, offset: int = 1, first_only=False):
    frames = inspect.stack()
    frames = frames[offset:]
    lst = []
    for f in frames:
        if "self" not in f.frame.f_locals:
            continue
        obj = f.frame.f_locals["self"]
        if not isinstance(obj, otype):
            continue
        if first_only:
            return obj
        lst.append(obj)

    if first_only:
        return None

    return lst


def deep_merge_dicts(target: dict, *args):
    assert isinstance(target, dict), "Target must be a dict"
    for src in args:
        assert isinstance(src, dict), "All merge items must be dicts"

        for key in src.keys():
            if key not in target:
                target[key] = src[key]
                continue
            if isinstance(src[key], dict) and isinstance(target[key], dict):
                target[key] = deep_merge_dicts({}, target[key], src[key])
            else:
                target[key] = src[key]
    return target


class style:
    GRAY = lambda x: colorize(str(x), "\033[90m")  # noqa: E731
    LIGHT_GRAY = lambda x: colorize(str(x), "\033[37m")  # noqa: E731
    BLACK = lambda x: colorize(str(x), "\033[30m")  # noqa: E731
    RED = lambda x: colorize(str(x), "\033[31m")  # noqa: E731
    GREEN = lambda x: colorize(str(x), "\033[32m")  # noqa: E731
    YELLOW = lambda x: colorize(str(x), "\033[33m")  # noqa: E731
    BLUE = lambda x: colorize(str(x), "\033[34m")  # noqa: E731
    MAGENTA = lambda x: colorize(str(x), "\033[35m")  # noqa: E731
    CYAN = lambda x: colorize(str(x), "\033[36m")  # noqa: E731
    WHITE = lambda x: colorize(str(x), "\033[97m")  # noqa: E731
    UNDERLINE = lambda x: colorize(str(x), "\033[4m")  # noqa: E731
    RESET = lambda x: colorize(str(x), "\033[0m")  # noqa: E731
