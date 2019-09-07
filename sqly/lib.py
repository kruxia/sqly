import asyncio
from importlib import import_module


def walk_list(a_list):
    """walk a nested list and yield items in a single stream"""
    for item in a_list:
        if not isinstance(item, str) and hasattr(item, '__iter__'):
            for i in flatten(item):
                yield i
        else:
            yield item


def flatten(a_list):
    """convert a walked list into a single list"""
    return list(walk_list(a_list))


def run(result):
    if asyncio.iscoroutine(result):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(result)
    return result


def get_settings(mod_name, settings_mod_name=None):
    if not settings_mod_name:
        settings_mod_name = f"{mod_name}.settings"
    return import_module(settings_mod_name)


def get_logging_settings(settings, log=None):
    logging_settings = settings.LOGGING if hasattr(settings, 'LOGGING') else {}
    if log:
        logging_settings.update(level=int(log))
    return logging_settings
