import asyncio


def walk(iterator):
    """
    Walk a nested iterator and yield items in a single stream
    """
    for item in iterator:
        # any non-string iterator needs to be recursed into
        if not isinstance(item, str) and hasattr(item, '__iter__'):
            for i in walk(item):
                yield i
        else:
            yield item


def run_sync(result):
    """
    If result is a coroutine, evaluate it
    """
    if asyncio.iscoroutine(result):
        return asyncio.run(result)
    else:
        return result
