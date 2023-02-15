import os

from lib.redis import RedisClient
from lib.pg import PgConnect


import os

class AppConfig:
    CERTIFICATE_PATH = '/crt/YandexInternalRootCA.crt'
    DEFAULT_JOB_INTERVAL = 25

    def redis_client(self) -> RedisClient:
		redis_host = 'c-c9qk44dr7ik3peg6bi8g.rw.mdb.yandexcloud.net'
		redis_port = 6380
		redis_password = 'Zaq12wsXCde3'
        return RedisClient(
            self.redis_host,
            self.redis_port,
            self.redis_password,
            self.redis_certificate = CERTIFICATE_PATH
        )

    def pg_warehouse_db(self):
		pg_warehouse_host = 'rc1b-107jwcgucbmzzozk.mdb.yandexcloud.net'
		pg_warehouse_port = 6432
		pg_warehouse_dbname = 'sprint9dwh'
		pg_warehouse_user = 's9sprint'
		pg_warehouse_password = 'Zaq12wsXCde3'
        return PgConnect(
            self.pg_warehouse_host,
            self.pg_warehouse_port,
            self.pg_warehouse_dbname,
            self.pg_warehouse_user,
            self.pg_warehouse_password
        )
