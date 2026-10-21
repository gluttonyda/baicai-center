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


import os
import socket as _socket

# 这些主机名要真实解析：真库用例靠它连测试用 MySQL 容器。
_DNS_PASSTHROUGH = {
    host
    for host in (
        os.getenv("BAICAI_TEST_MYSQL_HOST", ""),
        "baicai-review-mysql-20261021",
        "127.0.0.1",
        "localhost",
        "::1",
    )
    if host
}


@pytest.fixture(autouse=True)
def stub_dispatch_dns(monkeypatch):
    """派发前会解析出站主机名并做地址分类；默认给一个公网地址，避免用例依赖真实 DNS。

    需要真解析的主机（测试用 MySQL 容器）走透传；专测地址分类的用例在自己的 monkeypatch 里覆盖。
    """
    main_module = sys.modules.get("main")
    if main_module is not None:
        real_getaddrinfo = _socket.getaddrinfo

        def fake_getaddrinfo(host, port, *args, **kwargs):
            if isinstance(host, str) and host in _DNS_PASSTHROUGH:
                return real_getaddrinfo(host, port, *args, **kwargs)
            return [(2, 1, 6, "", ("203.0.113.10", port if isinstance(port, int) else 80))]

        monkeypatch.setattr(main_module.socket, "getaddrinfo", fake_getaddrinfo)
    yield
