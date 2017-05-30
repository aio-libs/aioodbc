import gc

from unittest import mock

import aioodbc
import pytest


@pytest.mark.parametrize('db', pytest.db_list)
@pytest.mark.asyncio
async def test___del__(event_loop, dsn, recwarn, executor):
    conn = await aioodbc.connect(dsn=dsn, loop=event_loop, executor=executor)
    exc_handler = mock.Mock()
    event_loop.set_exception_handler(exc_handler)

    del conn
    gc.collect()
    w = recwarn.pop()
    assert issubclass(w.category, ResourceWarning)

    msg = {'connection': mock.ANY,  # conn was deleted
           'message': 'Unclosed connection'}
    if event_loop.get_debug():
        msg['source_traceback'] = mock.ANY
    exc_handler.assert_called_with(event_loop, msg)
    assert not event_loop.is_closed()
