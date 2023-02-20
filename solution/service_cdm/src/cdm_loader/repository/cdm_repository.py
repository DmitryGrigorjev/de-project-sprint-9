from datetime import datetime
from typing import Dict
from lib.pg import PgConnect
from app_config import AppConfig

#классы для чтения и записи в/из Kafka
class KafkaProducer:
    def __init__(self, host: str, port: int, user: str, password: str, topic: str, cert_path: str) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'error_cb': error_callback,
        }

        self.topic = topic
        self.p = Producer(params)

    def produce(self, payload: Dict) -> None:
        self.p.produce(self.topic, json.dumps(payload))
        self.p.flush(10)
		
class KafkaConsumer:
    def __init__(self,host: str,port: int,user: str,password: str,topic: str,group: str,cert_path: str) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
          #  'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'group.id': group,  # '',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
          #  'error_cb': error_callback,
            'debug': 'all',
            'client.id': 'someclientkey'
        }

        self.topic = topic
        self.c = Consumer(params)
        self.c.subscribe([topic])

    def consume(self, timeout: float = 3.0) -> Optional[Dict]:        
        msg = self.c.poll(timeout=3.0)
        if not msg:
            return None
        if msg.error():
            raise Exception(msg.error())
        val = msg.value().decode()
        return json.loads(val)
		
class PgConnect:
    def __init__(self, host: str, port: int, db_name: str, user: str, pw: str, sslmode: str = "require", data: Dict) -> None:
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.sslmode = sslmode      
		self._db = db
		self._data = data
	#очистка первой витрины
	def UserCategoriesDelete(self) -> None:
			self._db = db
			with self._db.connection() as conn:
				with conn.cursor() as cur:
					cur.execute("""
								delete from cdm.user_category_counters
								""",
					)

	#вставка в первую витрину
	def UserCategoriesWrite(self) -> None:
			self._db = db
			with self._db.connection() as conn:
				with conn.cursor() as cur:
					cur.execute("""
								insert into cdm.user_category_counters
									(user_id, category_id, category_name, order_cnt)
								select
									hu.user_id,
									hc.h_category_pk,
									hc.category_name,
									count(*)as cnt
								from
									dds.h_user hu
								join dds.l_order_user lou on hu.h_user_pk = lou.h_user_pk
								join dds.l_order_product lop on	lou.h_order_pk = lop.h_order_pk
								join dds.l_product_category lpc on lpc.h_product_pk = lop.h_product_pk
								join dds.h_category hc on lpc.h_category_pk = hc.h_category_pk
								group by 
									hu.user_id,
									hc.h_category_pk,
									hc.category_name
								""",
					)	
					
	#очистка второй витрины
	def UserProductsDelete(self) -> None:
			self._db = db
			with self._db.connection() as conn:
				with conn.cursor() as cur:
					cur.execute("""
								delete from cdm.user_product_counters
								""",
					)

	#вставка во вторую витрину
	def UserProductsWrite(self) -> None:
			self._db = db
			with self._db.connection() as conn:
				with conn.cursor() as cur:
					cur.execute("""
								insert into cdm.user_product_counters
									(user_id, product_id, product_name, order_cnt)
								select
									hu.user_id,
									hp.product_id,
									spn.`name` as product_name,
									count(*)as cnt
								from
									dds.h_user hu
								join dds.l_order_user lou on
									hu.h_user_pk = lou.h_user_pk
								join dds.l_order_product lop on
									lou.h_order_pk = lop.h_order_pk
								join dds.h_product hp on
									lop.h_product_pk = hp.h_product_pk
								join dds.s_product_names spn on
									hp.h_product_pk = spn.h_product_pk
								group by 
									hu.user_id,
									hp.product_id,
									spn.`name`
								""",
					)
				
	#CDM слой заполнен


