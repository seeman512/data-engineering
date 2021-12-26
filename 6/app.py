import os
from config import Config
import logging

import psycopg2
from hdfs import InsecureClient
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from functools import partial


config_path = "config.yaml"
app_name = "app"
tables = ["actor", "address", "category", "city", "country", "customer",
          "film", "film_actor", "film_category", "inventory", "language",
          "payment", "rental", "staff", "store"]

class AppError(Exception):
    def __init__(self, message):
        self.message = f"Application error: {message}"
        super().__init__(self.message)


def db_to_hdfs(hdfs_client, pg_connect, directory):

    for table in tables:
        cursor = pg_connect.cursor()
        file_path = os.path.join(directory, f"{table}.csv")
        with hdfs_client.write(file_path, overwrite=True) as f:
            cursor.copy_expert(f"COPY (select * from {table}) TO STDOUT WITH HEADER CSV", f)
        logging.debug(f"Saved {file_path}")

def get_table_df(spark, directory, table):
    return spark.read.csv(os.path.join(directory, f"{table}.csv"),
                          header="true",
                          inferSchema="true")


if __name__ == "__main__":
    try:
        cfg = Config(config_path)

        logging.basicConfig(**cfg.logger_config())
        app_config = cfg.app_config(app_name)

        hdfs_client = InsecureClient(app_config["hdfs_url"])
        with psycopg2.connect(**app_config["pagila_db"]) as pg_connect:
            db_to_hdfs(hdfs_client, pg_connect, app_config["directory"])

        spark = SparkSession.builder.master('local').appName("dz_6").getOrCreate()

        get_df = partial(get_table_df, spark, app_config["directory"])

        # TABLES DFs
        film_df = get_df("film")
        film_category_df = get_df("film_category")
        category_df = get_df("category")
        actor_df = get_df("actor")
        film_actor_df = get_df("film_actor")
        inventory_df = get_df("inventory")
        city_df = get_df("city")
        address_df = get_df("address")
        customer_df = get_df("customer")
        rental_df = get_df("rental")
        payment_df = get_df("payment")

        # JOIN DFs
        film_vs_category_df = film_df\
            .join(film_category_df, film_df.film_id == film_category_df.film_id, "inner")\
            .join(category_df, category_df.category_id == film_category_df.category_id, "inner")
        film_vs_actor_df = actor_df\
            .join(film_actor_df, film_actor_df.actor_id == actor_df.actor_id, "inner")\
            .join(film_df, film_df.film_id == film_actor_df.film_id, "inner")
        children_category_actor_df = category_df\
            .where(category_df.name == "Children")\
            .join(film_category_df, film_category_df.category_id == category_df.category_id, "inner")\
            .join(film_df, film_df.film_id == film_category_df.film_id, "inner")\
            .join(film_actor_df, film_actor_df.film_id == film_category_df.film_id, "inner")\
            .join(actor_df, actor_df.actor_id == film_actor_df.actor_id, "inner")\
            .groupBy(actor_df.actor_id, actor_df.first_name, actor_df.last_name)\
            .count()
        categories_with_a_and_dash_cities_df = film_vs_category_df\
            .join(inventory_df, film_df.film_id == inventory_df.film_id, "inner")\
            .join(rental_df, rental_df.inventory_id == inventory_df.inventory_id, "inner")\
            .join(customer_df, customer_df.customer_id == rental_df.customer_id, "inner")\
            .join(address_df, address_df.address_id == customer_df.address_id, "inner")\
            .join(city_df, city_df.city_id==address_df.city_id, "inner")\
            .where(F.lower("city").contains("a") | F.col("city").contains("-"))\
            .groupBy(category_df.category_id, category_df.name)\
            .agg(
                F.sum(F.when(F.lower("city").startswith("a"), film_df.rental_duration).otherwise(0)).alias('a_amount'),
                F.sum(F.when(F.col("city").contains("-"), film_df.rental_duration).otherwise(0)).alias('dash_amount')
            )

        # RESULTS
        results = [{
            "msg": "вывести количество фильмов в каждой категории, отсортировать по убыванию",
            "df": film_vs_category_df
                    .groupBy(category_df.category_id, category_df.name)
                    .count()
                    .orderBy(F.desc("count"))
                    .select("name", "count")
            }, {
            "msg": "вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию",
            "df": film_vs_actor_df
                    .groupBy(actor_df.actor_id,
                             actor_df.first_name,
                             actor_df.last_name)
                    .agg(F.sum(film_df.rental_duration).alias("duration"))
                    .orderBy(F.desc("duration"))
                    .limit(10)
            }, {
            "msg": "вывести категорию фильмов, на которую потратили больше всего денег",
            "df" : film_vs_category_df
                    .join(inventory_df, inventory_df.film_id == film_df.film_id, "inner")
                    .join(rental_df, rental_df.inventory_id == inventory_df.inventory_id, "inner")
                    .join(payment_df, payment_df.rental_id == rental_df.rental_id, "inner")
                    .groupBy(category_df.name)
                    .agg(F.sum(payment_df.amount).alias("amount"))
                    .orderBy(F.desc("amount"))
                    .limit(1)
                    .select("name", F.round("amount", 2).alias("amount"))
            }, {
            "msg": "вывести названия фильмов, которых нет в inventory.",
            "df": film_df
                    .join(inventory_df, film_df.film_id==inventory_df.film_id, "left")
                    .where(inventory_df.film_id.isNull())
                    .select(film_df.title)
            }, {
            "msg": "вывести топ 3 актеров, которые больше всего появлялись "
                    + "в фильмах в категории “Children”."
                    + "Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.",
            "df": children_category_actor_df
                    .join(children_category_actor_df
                            .select("count")
                            .distinct()
                            .orderBy(F.desc("count"))
                            .limit(3),
                          ["count"],
                          "inner")
                    .orderBy(F.desc("count"))
            }, {
            "msg": "вывести города с количеством активных и неактивных клиентов "
                    + "(активный — customer.active = 1). "
                    + "Отсортировать по количеству неактивных клиентов по убыванию.",
            "df": city_df
                    .join(address_df, city_df.city_id == address_df.city_id, "inner")
                    .join(customer_df, customer_df.address_id == address_df.address_id, "inner")
                    .groupBy(city_df.city_id, city_df.city)
                    .agg(
                        F.sum(F.when(customer_df.active == 1, 1).otherwise(0)).alias('active_users'),
                        F.sum(F.when(customer_df.active == 0, 1).otherwise(0)).alias('inactive_users'))
                    .orderBy(F.desc("inactive_users"), "city")
            }, {
            "msg": "вывести категорию фильмов, у которой самое большое кол-во часов "
                    + "суммарной аренды в городах (customer.address_id в этом city), "
                    + "и которые начинаются на букву “a”. "
                    + "То же самое сделать для городов в которых есть символ “-”.",
            "df": categories_with_a_and_dash_cities_df
                    .orderBy(F.desc("a_amount"))
                    .limit(1)
                    .select("name", F.col("a_amount").alias("amount"))
                    .withColumn("details", F.lit("cities with 'a'"))
                    .union(
                        categories_with_a_and_dash_cities_df
                            .orderBy(F.desc("dash_amount"))
                            .limit(1)
                            .select("name", F.col("dash_amount").alias("amount"))
                            .withColumn("details", F.lit("cities with '-'")))
            }
        ]

        for result in results:
            print(f"\n=========== {result['msg']} ========")
            result["df"].show() 

    except AppError as e:
        logging.error(e)
    except Exception as e:
        logging.error(e)
