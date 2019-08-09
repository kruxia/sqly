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
