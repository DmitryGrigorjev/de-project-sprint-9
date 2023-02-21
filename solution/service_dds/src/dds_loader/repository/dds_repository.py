from datetime import datetime
from typing import Dict
from lib.pg import PgConnect
from lib.redis import RedisClient
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

class RedisClient:
    def __init__(self, host: str, port: int, password: str, cert_path: str) -> None:
        self._client = redis.StrictRedis(	
											host=host,
											port=port,
											password=password,
											ssl=True,
											ssl_ca_certs=cert_path
										)
	def get(self) -> Dict:
		for k in self._client.keys():
			str = self._client.get(k)  # type: ignore
			data = append(json.loads(str))		
		return data
		
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
	#для чтения из STG слоя postgre
	def get(self)->Dict:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
				cur.execute("""
							SELECT object_id, payload 
							FROM stg.order_events
							""",
				)	
			data=cur.fetchall()			
 		return data
	#в h_category просто вставляем строку
    def h_category(self, category_name: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
							INSERT INTO dds.h_category
							(category_name, load_src)
							VALUES (%(category_name)s, 'redis')
							""",
							{
								'category_name': category_name,
							}
                )
	#в h_usrer вставляем id
	def h_user_insert(self, user_id: str) -> None:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
			cur.execute("""
						INSERT INTO dds.h_user
						(user_id, load_src)
						VALUES (%(user_id)s, 'redis')
						""",
						{
							'user_id': user_id,
						}
			)
	#в s_user_names - логин и имя			
	def s_user_names_insert(self, user_id: str, username: str, userlogin:str) -> None:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
			cur.execute("""
						INSERT INTO dds.s_user_names
						(h_user_pk, username, userlogin, load_src)
						VALUES (%(user_id)s, %(username)s, %(userlogin)s, 'redis')
						""",
						{
							'user_id': user_id,
							'username': username,
							'userlogin': userlogin,
						}
			)
	#в h_restaurant также только id
	def h_restaurant_insert(self, restaurant_id: str) -> None:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
			cur.execute("""
						INSERT INTO dds.h_restaurant
						(restaurant_id, load_src)
						VALUES (%(restaurant_id)s, 'redis')
						""",
						{
							'restaurant_id': restaurant_id,
						}
			)
	#и в s_restaurant_names			
	def s_restaurant_names_insert(self, restaurant_id: str, name: str) -> None:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
			cur.execute("""
						INSERT INTO dds.s_restaurant_names
						(h_restaurant_pk, name, load_src)
						VALUES (%(restaurant_id)s, %(name)s, 'redis')
						""",
						{
							'restaurant_id': restaurant_id,
							'name': name,
						}
			)
	#еще надо заполнить h_product			
	def h_product_insert(self, product_id: str) -> None:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
			cur.execute("""
						INSERT INTO dds.h_product
						(product_id, load_src)
						VALUES (%(product_id)s, 'redis')
						""",
						{
							'product_id': product_id,
						}
			)
	#и s_product_names
	def s_product_names_insert(self, product_id: str, name: str) -> None:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
			cur.execute("""
						INSERT INTO dds.s_product_names
						(h_product_pk, name, load_src)
						VALUES (%(product_id)s, %(name)s, 'redis')
						""",
						{
							'product_id': product_id,
							'name': name,
						}
			)	
	#для поиска тех категорий, которые уже есть в h_category чтобы не задвоились записи при вставке
	# в данной итерации, для последующих вставок данные должны сохраняться для целостности, отфильтровать можно будет по дате
	def h_category_select(self, category_name: str) -> str:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
				cur.execute("""
							SELECT category_name 
							FROM dds.h_category
							WHERE category_name = %(category_name)s
							""",
							{
								'category_name': category_name
							}
				)	
			category=cur.fetchall()
 			return category
			
	#теперь заполнение таблиц линков product_category и product_restaurant			
	def l_product_category_insert(self, product_id: str, category_id: str) -> None:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
			cur.execute("""
						INSERT INTO dds.l_product_category
						(h_product_pk, h_category_pk, load_src)
						VALUES (%(product_id)s,(%(category_id)s, 'redis')
						""",
						{
							'product_id': product_id,
							'category_id': category_id,
						}
			)
	def l_product_restaurant_insert(self, product_id: str, restaurant_id: str) -> None:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
			cur.execute("""
						INSERT INTO dds.l_product_restaurant
						(h_product_pk, h_restaurant_pk, load_src)
						VALUES (%(product_id)s,(%(restaurant_id)s, 'redis')
						""",
						{
							'product_id': product_id,
							'restaurant_id': restaurant_id,
						}
			)

	#заполнение h_order			
	def h_order_insert(self, order_id: str, order_dt: datetime) -> None:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
			cur.execute("""
						INSERT INTO dds.h_order
						(order_id, order_dt, load_src)
						VALUES (%(order_id)s,%(order_dt)s,'postgre')
						""",
						{
							'order_id': order_id,
							'order_dt': order_dt,
						}
			)
	#заполнение сателлитов order			
	def s_order_status_insert(self, order_id: str, status: str) -> None:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
			cur.execute("""
						INSERT INTO dds.s_order_status
						(order_id, status, load_src)
						VALUES (%(order_id)s,%(status)s,'postgre')
						""",
						{
							'order_id': order_id,
							'status': status,
						}
			)
	def s_order_cost(self, order_id: str, cost: float, payment: float) -> None:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
			cur.execute("""
						INSERT INTO dds.s_order_cost
						(order_id, cost, payment, load_src)
						VALUES (%(order_id)s,%(cost)s,%(payment)s,'postgre')
						""",
						{
							'order_id': order_id,
							'cost': cost,
							'payment': payment,
						}
			)
	#и таблицы линки
	def l_order_user_insert(self, order_id: str, user_id: str) -> None:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
			cur.execute("""
						INSERT INTO dds.l_order_user
						(h_order_pk, h_user_pk, load_src)
						VALUES (%(order_id)s,(%(user_id)s, 'postgre')
						""",
						{
							'order_id': order_id,
							'user_id': user_id,
						}
			)
	def l_order_product_insert(self, product_id: str, order_id: str) -> None:
		with self._db.connection() as conn:
			with conn.cursor() as cur:
			cur.execute("""
						INSERT INTO dds.l_order_product
						(h_product_pk, h_order_pk, load_src)
						VALUES (%(product_id)s,(%(order_id)s, 'postgre')
						""",
						{
							'product_id': product_id,
							'order_id': order_id,
						}
			)
	#чтение данных для первой витрины
	def UserCategoriesRead(self) -> Dict:
			self._db = db
			with self._db.connection() as conn:
				with conn.cursor() as cur:
					cur.execute("""
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
					data=cur.fetchall()			
			return data

	#чтение данных для второй витрины
	def UserProductsRead(self) -> None:
			self._db = db
			with self._db.connection() as conn:
				with conn.cursor() as cur:
					cur.execute("""
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
					data=cur.fetchall()			
			return data

