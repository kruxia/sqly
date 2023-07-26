def walk(iterator):
    """
    Walk a nested iterator and yield items in a single stream.

    Examples:
        >>> l = [1, [2, [3, [4, 5, 6], 7, [8, 9], 10], 11]]
        >>> list(walk(l))
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    """
    for item in iterator:
        # any non-string iterator needs to be recursed into
        if not isinstance(item, str) and hasattr(item, "__iter__"):
            for i in walk(item):
                yield i
        else:
            yield item
