from collections.abc import Coroutine


class _CursorContextManager(Coroutine):

    __slots__ = ('_coro', '_cursor')

    def __init__(self, coro):
        self._coro = coro
        self._cursor = None

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

    def __await__(self):
        resp = yield from self._coro
        return resp

    async def __aenter__(self):
        self._cursor = await self._coro
        return self._cursor

    async def __aexit__(self, exc_type, exc, tb):
        await self._cursor.close()
