def cast(value, astype, default=None):
    """
    Convert a value from one type to another
    """
    try:
        return astype(value)
    except (TypeError, ValueError):
        pass
    return default


def coalesce(*values):
    """
    Return the first non-None value or None if all values are None
    """
    for value in values:
        if value is not None:
            return value
    return None


def get(collection, path, default=None, separator="."):
    """
    Get a value from a nested collection
    """
    keys = split_path(path, separator)
    value = collection
    for key in keys:
        try:
            value = value[key]
        except (KeyError, IndexError, TypeError):
            return default
    return value if value is not None else default


def get_coalesced(collection, paths, default=None, separator="."):
    """
    Return the first non-None value from a list of selected items
    of a nested collection
    """
    for path in paths:
        value = get(collection, path, default, separator)
        if value is not None:
            return value
    return None


def put(collection, path, value, separator="."):
    """
    Set a value in a nested collection and create parent nodes if needed
    """
    if isinstance(path, str):
        path = split_path(path, separator)
    for key in path[:-1]:
        if isinstance(collection, dict):
            collection = collection.setdefault(key, {})
        else:
            collection = collection[key]
    collection[path[-1]] = value


def split_path(path, separator=".", stripped=True):
    """
    Split a path into a list of segments; optionally remove leading and
    trailing separator characters
    """
    if stripped:
        path = path.strip(separator)
    return path.split(separator)
