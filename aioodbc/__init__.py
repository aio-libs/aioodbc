import asyncio
import warnings

from pyodbc import dataSources as _dataSources

from .connection import Connection, connect
from .pool import Pool, create_pool

__version__ = "0.4.0"
__all__ = ["connect", "Connection", "create_pool", "Pool", "dataSources"]


async def dataSources(loop=None, executor=None):
    """Returns a dictionary mapping available DSNs to their descriptions.

    :param loop: asyncio compatible event loop
    :param executor: instance of custom ThreadPoolExecutor, if not supplied
        default executor will be used
    :return dict: mapping of dsn to driver description
    """
    if loop is not None:
        msg = "Explicit loop is deprecated, and has no effect."
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
    loop = asyncio.get_event_loop()
    sources = await loop.run_in_executor(executor, _dataSources)
    return sources
