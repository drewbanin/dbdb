
def none_if_empty(func):
    def _inner(values):
        lst = list(values)
        if len(lst) == 0:
            return None

        return func(lst)
    return _inner


@none_if_empty
def agg_min(values):
    return min(values)


@none_if_empty
def agg_max(values):
    return max(values)


@none_if_empty
def agg_sum(values):
    return sum(values)


@none_if_empty
def agg_avg(values):
    count = len(values)
    total = sum(values)

    if count == 0:
        return None
    else:
        return total / count


@none_if_empty
def agg_count(values):
    return sum(0 if v is None else 1 for v in values)


@none_if_empty
def agg_countd(values):
    return len(set(values))


@none_if_empty
def agg_list(values, distinct=False):
    # TODO: distinct, ordering, w/e
    return ",".join(values)
