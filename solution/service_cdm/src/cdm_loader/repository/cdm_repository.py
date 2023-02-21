from datetime import datetime
from typing import Dict
from lib.pg import PgConnect
from app_config import AppConfig

#классы для чтения из Kafka
		
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
	def UserCategoriesWrite(self, user_id: int, category_id: int, category_name: str, order_cnt: int) -> None:
			self._db = db
			with self._db.connection() as conn:
				with conn.cursor() as cur:
					cur.execute("""
								insert into cdm.user_category_counters
									(user_id, category_id, category_name, order_cnt)
								values (%(user_id)s, %(category_id)s, %(category_name)s, %(order_cnt)s)
								""",
								{
								'user_id' = user_id, 
								'category_id' = category_id, 
								'category_name' = category_name, 
								'order_cnt' = order_cnt
								}
					)	
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
	def UserProductsWrite(self, user_id: int, product_id: int, product_name: str, order_cnt: int) -> None:
			self._db = db
			with self._db.connection() as conn:
				with conn.cursor() as cur:
					cur.execute("""
								insert into cdm.user_product_counters
									(user_id, product_id, product_name, order_cnt)
								values (%(user_id)s, %(product_id)s, %(product_name)s, %(order_cnt)s)
								""",
								{
								'user_id' = user_id, 
								'product_id' = product_id, 
								'product_name' = product_name, 
								'order_cnt' = order_cnt
								}
					)	
                )
	#CDM слой заполнен


