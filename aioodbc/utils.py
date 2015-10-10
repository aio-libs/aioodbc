import asyncio
from collections.abc import Coroutine


class _ContextManager(Coroutine):

    __slots__ = ('_coro', '_obj')

    def __init__(self, coro):

        self._coro = coro
        self._obj = None

    def send(self, value):
        return self._coro.send(value)

    def throw(self, typ, val=None, tb=None):
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        else:
            return self._coro.throw(typ, val, tb)

    def close(self):
        return self._coro.close()

    @asyncio.coroutine
    def _wrap_async_func(self):
        r = yield from self._coro
        return r

    def __await__(self):
        resp = yield from self._wrap_async_func()
        return resp

    async def __aenter__(self):
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, exc_type, exc, tb):
        await self._obj.close()


class _PoolContextManager(_ContextManager):
    async def __aexit__(self, exc_type, exc, tb):
        self._obj.close()
        await self._obj.wait_closed()
