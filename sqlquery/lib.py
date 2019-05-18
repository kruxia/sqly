def flatten(a_list):
    """flatten a nested list into a single stream"""
    for item in a_list:
        if isinstance(item, list):
            for i in flatten(item):
                yield i
        else:
            yield item
