from types import TracebackType
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    Generator,
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
)

from pyodbc import Error

# Issue #195.  Don't pollute the pool with bad conns
# Unfortunately occasionally sqlite will return 'HY000' for invalid query,
# so we need specialize the check
_CONN_CLOSE_ERRORS: Dict[str, Union[str, None]] = {
    # [Microsoft][ODBC Driver 17 for SQL Server]Communication link failure
    "08S01": None,
    # [HY000] server closed the connection unexpectedly
    "HY000": "[HY000] server closed the connection unexpectedly",
}


def _is_conn_close_error(e: Exception) -> bool:
    if not isinstance(e, Error) or len(e.args) < 2:
        return False

    sqlstate, msg = e.args[0], e.args[1]
    if sqlstate not in _CONN_CLOSE_ERRORS:
        return False

    check_msg = _CONN_CLOSE_ERRORS[sqlstate]
    if not check_msg:
        return True

    return str(msg).startswith(check_msg)


_TObj = TypeVar("_TObj")
_Release = Callable[[_TObj], Awaitable[None]]


class _ContextManager(Coroutine[Any, None, _TObj], Generic[_TObj]):
    __slots__ = ("_coro", "_obj", "_release", "_release_on_exception")

    def __init__(
        self,
        coro: Coroutine[Any, None, _TObj],
        release: _Release[_TObj],
        release_on_exception: Optional[_Release[_TObj]] = None,
    ):
        self._coro = coro
        self._obj: Optional[_TObj] = None
        self._release = release
        self._release_on_exception = (
            release if release_on_exception is None else release_on_exception
        )

    def send(self, value: Any) -> "Any":
        return self._coro.send(value)

    def throw(  # type: ignore
        self,
        typ: Type[BaseException],
        val: Optional[Union[BaseException, object]] = None,
        tb: Optional[TracebackType] = None,
    ) -> Any:
        if val is None:
            return self._coro.throw(typ)
        if tb is None:
            return self._coro.throw(typ, val)
        return self._coro.throw(typ, val, tb)

    def close(self) -> None:
        self._coro.close()

    def __await__(self) -> Generator[Any, None, _TObj]:
        return self._coro.__await__()

    async def __aenter__(self) -> _TObj:
        self._obj = await self._coro
        assert self._obj
        return self._obj

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if self._obj is None:
            return

        try:
            if exc_type is not None:
                await self._release_on_exception(self._obj)
            else:
                await self._release(self._obj)
        finally:
            self._obj = None
