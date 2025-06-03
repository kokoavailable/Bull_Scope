# collectors/base.py
import threading
from psycopg2.pool import ThreadedConnectionPool
from helper.common import DB_PARAMS, logger
from typing import List, Dict

class BaseCollector:
    _lock = threading.Lock()
    connection_pool = None

    def __init__(self, minconn=4, maxconn=16):

        logger.info(f"current db param : {DB_PARAMS}")
        with self.__class__._lock:
            if self.__class__.connection_pool is None:
                self.__class__.connection_pool = ThreadedConnectionPool(
                    minconn, maxconn, **DB_PARAMS
                )

    def _get_stock_id_map(self, symbols: List[str]) -> Dict[str, int]:
        """
        주어진 symbols 리스트에 대해 한번에 {symbol: stock_id} 매핑을 반환
        DB에 없는 심볼은 맵에 포함되지 않음.
        """
        if not symbols:
            return {}

        conn = self._get_conn()
        cur = conn.cursor()
        try:
            placeholders = ",".join(["%s"] * len(symbols))
            sql = f"""
                SELECT symbol, id
                  FROM stocks
                 WHERE symbol IN ({placeholders});
            """
            cur.execute(sql, symbols)
            return {symbol: sid for symbol, sid in cur.fetchall()}
        finally:
            cur.close()
            self._put_conn(conn)

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
