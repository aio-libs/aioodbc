import asyncio
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional

from pyodbc import dataSources as _dataSources

from ._version import version, version_tuple
from .connection import Connection, connect
from .cursor import Cursor
from .pool import Pool, create_pool

__version__ = version
__version_tuple__ = version_tuple
__all__ = (
    "connect",
    "Connection",
    "create_pool",
    "Pool",
    "dataSources",
    "Cursor",
)


async def dataSources(
    loop: Optional[asyncio.AbstractEventLoop] = None,
    executor: Optional[ThreadPoolExecutor] = None,
) -> Dict[str, str]:
    """Returns a dictionary mapping available DSNs to their descriptions.

    :param loop: asyncio compatible event loop, deprecated
    :param executor: instance of custom ThreadPoolExecutor, if not supplied
        default executor will be used
    :return dict: mapping of dsn to driver description
    """
    if loop is not None:
        msg = "Explicit loop is deprecated, and has no effect."
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
    loop = asyncio.get_event_loop()
    sources: Dict[str, str] = await loop.run_in_executor(
        executor, _dataSources
    )
    return sources
