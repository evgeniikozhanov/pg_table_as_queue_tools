from pg_table_as_queue_tools import get_sql_to_get_batch_from_queue, get_sql_to_release_queue_item, \
    get_sql_to_delete_queue_item


def test_get_sql_to_get_batch_from_queue():
    sql_expected  = """WITH x AS (
      SELECT id FROM queue.send WHERE queue_item_state != 'blocked' OR next_request_datetime <= NOW()
      LIMIT 10 FOR UPDATE SKIP LOCKED
    )
    UPDATE queue.send
        SET next_request_datetime = NOW() + INTERVAL '60 seconds',
        queue_item_state = 'blocked'
    FROM x
        WHERE queue.send.id = x.id
    RETURNING *
    """
    sql = get_sql_to_get_batch_from_queue('queue.send', 'id', 'next_request_datetime', 10, 60, 'queue_item_state')
    assert sql == sql_expected


def test_get_sql_to_release_queue_item():
    sql_expected = """UPDATE queue.send
        SET next_request_datetime = 'NOW()', queue_item_state = 'released'
        WHERE id = '555'
    """
    sql = get_sql_to_release_queue_item('queue.send', 'id', '555', 'next_request_datetime', 'queue_item_state')
    assert sql == sql_expected


def test_get_sql_to_delete_queue_item():
    sql_expected = """
        DELETE FROM queue.send
        WHERE id = '555'
    """
    sql = get_sql_to_delete_queue_item('queue.send', 'id', '555')
    assert sql == sql_expected
