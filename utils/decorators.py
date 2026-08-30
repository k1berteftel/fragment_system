import asyncio
from functools import wraps


def retry_decorator(max_retries=1, delay=15):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            counter = 0
            while True:
                if counter >= max_retries:
                    return None
                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception:
                    counter += 1
                    await asyncio.sleep(delay)
        return wrapper
    return decorator