# collectors/base.py
import threading
from psycopg2.pool import ThreadedConnectionPool
from helper.common import DB_PARAMS

class BaseCollector:
    _lock = threading.Lock()
    connection_pool = None

    def __init__(self, minconn=4, maxconn=16):
        with self.__class__._lock:
            if self.__class__.connection_pool is None:
                self.__class__.connection_pool = ThreadedConnectionPool(
                    minconn, maxconn, **DB_PARAMS
                )

    def _get_conn(self):
        return self.__class__.connection_pool.getconn()

    def _put_conn(self, conn):
        self.__class__.connection_pool.putconn(conn)

    def _ensure_table(self):
        """(서브클래스에서 override)"""
        pass

    @classmethod
    def close_pool(cls):
        if cls.connection_pool:
            cls.connection_pool.closeall()
