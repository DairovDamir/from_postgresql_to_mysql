import pandas as pd
import logging
import psycopg2
import mysql.connector

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def postgre_conn(admin, password, port, dbname):
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=port,
            dbname=dbname,
            user=admin,
            password=password
        )
        logging.info("Успешное подключение к PostgreSQL")
        return conn

    except psycopg2.OperationalError as e:
        logging.error(f"Ошибка подключения к PostgreSQL: {e}")
        raise
        return None

    except Exception as e:
        logging.exception("Неожиданная ошибка")
        return None

def mysql_conn(admin, password, port, dbname):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            port=port,
            database=dbname,
            user=admin,
            password=password
        )
        logging.info("Успешное подключение к MySQL")
        return conn

    except mysql.connector.Error as e:
        logging.error(f"Ошибка подключения к MySQL: {e}")
        return None

    except Exception as e:
        logging.exception("Неожиданная ошибка")
        return None

def create_table(write_conn):
    cursor = write_conn.cursor()
    try:
        query = """
                CREATE TABLE IF NOT EXISTS passenger_mart (
                    passenger_id text,
                    passenger_name text,
                    city_to text,
                    total_revenue_by_city int,
                    cnt_of_flights_to_city int,
                    total_revenue int,
                    cnt_of_flights int,
                    percent_of_revenue_by_cities decimal(10, 1),
                    percent_of_flights_by_cities decimal(10, 1)
                );
                """
        cursor.execute(query)
        write_conn.commit()

        logging.info("Таблица успешно создана")

    except Exception:
        write_conn.rollback()
        logging.exception("Ошибка при создании таблицы")

def write_mysql(mart, read_conn, write_conn):
    try:
        create_table(write_conn)


    except Exception:
        write_conn.rollback()
        logging.exception("Не удалось создать таблицу")
        return

    try:
        df = pd.read_sql(f"SELECT * FROM {mart};", read_conn)

        columns = ", ".join([f"`{col}`" for col in df.columns])
        placeholders = ", ".join(["%s"] * len(df.columns))

        query = f"INSERT INTO passenger_mart ({columns}) VALUES ({placeholders})"

        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        batch_size = 10000

        cursor = write_conn.cursor()

        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            cursor.executemany(query, batch)
            write_conn.commit()
            logging.info(f"Загружено {min(i + batch_size, len(data))} из {len(data)} строк")

        logging.info("Данные загружены")
    except Exception:
        write_conn.rollback()
        logging.exception("Ошибка при загрузке")
    finally:
        cursor.close()
        read_conn.close()
        write_conn.close()


postgre_conn = postgre_conn('postgres', 'postgres', '5432', 'demo')

df = pd.read_sql("SELECT * FROM passenger_mart LIMIT 10;", postgre_conn)

mysql_conn = mysql_conn('user', 'user', '3306', 'demo')

write_mysql('passenger_mart', postgre_conn, mysql_conn)





