import sys
from unittest.mock import MagicMock, patch

import pytest


def make_fake_dispatch_lock_connection(acquired: int = 1) -> MagicMock:
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = {"acquired": acquired}
    return connection


@pytest.fixture(autouse=True)
def fake_dispatch_db_lock(request):
    """单元测试默认不连接真实 MySQL；真库测试用 real_dispatch_db_lock 标记关闭替身。"""
    main_module = sys.modules.get("main")
    if main_module is None or request.node.get_closest_marker("real_dispatch_db_lock"):
        yield None
        return
    with patch.object(
        main_module,
        "_open_dispatch_lock_connection",
        side_effect=lambda: make_fake_dispatch_lock_connection(),
        create=True,
    ) as fake_open:
        yield fake_open
