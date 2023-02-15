from datetime import datetime
from typing import Dict
from lib.pg import PgConnect
from app_config import AppConfig

#очистка первой витрины
class UserCategoriesDelete:
    def __init__(self, db: PgConnect) -> None:
		self._db = db
		with self._db.connection() as conn:
			with conn.cursor() as cur:
				cur.execute("""
							delete from cdm.user_category_counters
							""",
				)

#вставка в первую витрину
class UserCategoriesWrite:
    def __init__(self, db: PgConnect) -> None:
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
class UserProductsDelete:
    def __init__(self, db: PgConnect) -> None:
		self._db = db
		with self._db.connection() as conn:
			with conn.cursor() as cur:
				cur.execute("""
							delete from cdm.user_product_counters
							""",
				)

#вставка во вторую витрину
class UserProductsWrite:
    def __init__(self, db: PgConnect) -> None:
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


