def _object_fqn(obj: object) -> str:
    if hasattr(obj, "__name__"):
        return f"{obj.__module__}.{obj.__name__}"
    return f"{obj.__module__}.{obj.__class__.__name__}"
