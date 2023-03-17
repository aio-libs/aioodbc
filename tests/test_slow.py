import gc
from unittest import mock

import pytest

import aioodbc


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test___del__(loop, dsn, recwarn, executor):
    conn = await aioodbc.connect(dsn=dsn, executor=executor)
    exc_handler = mock.Mock()
    loop.set_exception_handler(exc_handler)

    del conn
    gc.collect()
    w = recwarn.pop()
    assert issubclass(w.category, ResourceWarning)

    msg = {
        "connection": mock.ANY,  # conn was deleted
        "message": "Unclosed connection",
    }
    if loop.get_debug():
        msg["source_traceback"] = mock.ANY
    exc_handler.assert_called_with(loop, msg)
    assert not loop.is_closed()
