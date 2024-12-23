"""Data utils"""
import functools
import collections
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from collections import OrderedDict
import src.config
import concurrent.futures
from mysql.connector import pooling

@functools.lru_cache(maxsize=1024)
def get_pool_db():
    db_pool = pooling.MySQLConnectionPool(
    pool_name="my_pool",
    pool_size=5,
    host=src.config.HOST,
    database=src.config.DATABASE_STIR,
    user=src.config.USER,
    password=src.config.PWD
)
    return db_pool


def master_database():
    """
    :return:
    """
    db_password = "DmUKR0yONjMExVNN"
    uri = (f"mongodb+srv://admin:{db_password}@cluster0.vg3mk.mongodb.net"
           f"/?retryWrites=true&w=majority&appName=Cluster0")
    client = MongoClient(uri, server_api=ServerApi('1'))
    return client["max_entropy"]

def fed_speech_structured_output():
    return master_database()["fed_speech_structured_output"]

def fed_speech_collection():
    """
    :return:
    """
    return master_database()["fed_speech_summary"]

def __fedfund_volume_decomposition_wrapper():
    db_pool = get_pool_db()
    connection = db_pool.get_connection()
    cursor = connection.cursor(dictionary=True)
    table_name = src.config.TABLE_FF_DECOMP_VOLUME
    cursor.execute(f"SELECT * FROM {table_name}")
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

@functools.lru_cache(maxsize=1024)
def read_fedfund_volume_decomposition_table():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(__fedfund_volume_decomposition_wrapper)
        return_value = future.result()
    return return_value

def __fred_related_wrapper():
    db_pool = get_pool_db()
    connection = db_pool.get_connection()
    cursor = connection.cursor(dictionary=True)
    result = {}
    for table_name in [src.config.TABLE_EFFR, src.config.TABLE_RRP_VOLUME,
                    src.config.TABLE_RRP_RATE, src.config.TABLE_FOREIGN_RRP,
                    src.config.TABLE_LOWER_BOUND, src.config.TABLE_UPPER_BOUND,
                    src.config.TABLE_RESERVE_BALANCE,
                    src.config.TABLE_IORB, src.config.TABLE_TGA_BALANCE]:
        cursor.execute(f"SELECT * FROM {table_name}")
        data = cursor.fetchall()
        time_series = collections.OrderedDict()
        for row in data:
            time_series[row["date"]] = row["value"]
        time_series = dict(sorted(time_series.items()))
        result[table_name] = time_series
    cursor.close()
    connection.close()
    return result

@functools.lru_cache(maxsize=1024)
def __all_fred_tables():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(__fred_related_wrapper)
        return_value = future.result()
    return return_value


@functools.lru_cache(maxsize=32)
def read_fred_related_table(table_name):
    return __all_fred_tables()[table_name]

def __ofr_data_wrapper():
    db_pool = get_pool_db()
    connection = db_pool.get_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {src.config.TABLE_OFR_ON_DATA}")
    data = cursor.fetchall()
    result = {}
    for row in data:
        date = row["date"]
        for key in row:
            if key == "date":
                continue
            if key not in result:
                result[key] = OrderedDict()
            result[key][date] = row[key]
    for key in result:
        result[key] = dict(sorted(result[key].items()))
    cursor.close()
    connection.close()
    return result

def __treasury_data_wrapper(sec_type, data_type):
    db_pool = get_pool_db()
    connection = db_pool.get_connection()
    cursor = connection.cursor(dictionary=True)

    table_name = f"TABLE_{sec_type.upper().replace(' ', '')}_OUTSTANDING_AMOUNT"
    cursor.execute(f"SELECT * FROM {table_name}")
    data = cursor.fetchall()
    result = {}
    for row in data:
        date = row["date"]
        for key in row:
            if key == "date":
                continue
            if key not in result:
                result[key] = OrderedDict()
            result[key][date] = row[key]
    for key in result:
        result[key] = dict(sorted(result[key].items()))
    cursor.close()
    connection.close()
    return result

@functools.lru_cache()
def read_ofr_data_table():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(__ofr_data_wrapper)
        return_value = future.result()
    return return_value

@functools.lru_cache()
def read_treasury_data(sec_type, data_type):
    args = [sec_type, data_type]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(__treasury_data_wrapper, *args)
        return_value = future.result()
    return return_value["value"]

@functools.lru_cache(maxsize=32)
def __daylight_overdraft_wrapper():
    db_pool = get_pool_db()
    connection = db_pool.get_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {src.config.TABLE_DAYLIGHT_OVERDFRAT}")
    data = cursor.fetchall()
    result = {}
    for row in data:
        date = row["date"]
        for key in row:
            if key == "date":
                continue
            if key not in result:
                result[key] = OrderedDict()
            result[key][date] = row[key]
    for key in result:
        result[key] = dict(sorted(result[key].items()))
    cursor.close()
    connection.close()
    return result

def read_daylight_overdraft_table():

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(__daylight_overdraft_wrapper)
        return_value = future.result()
    return return_value


def __elasticity_wrapper():
    db_pool = get_pool_db()
    connection = db_pool.get_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {src.config.TABLE_ELASTICITY}")
    data = cursor.fetchall()
    result = {}
    for row in data:
        date = row["date"]
        for key in row:
            if key == "date":
                continue
            if key not in result:
                result[key] = OrderedDict()
            result[key][date] = row[key]
    for key in result:
        result[key] = dict(sorted(result[key].items()))

    cursor.close()
    connection.close()

    return result

def read_elasticity_table():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(__elasticity_wrapper)
        return_value = future.result()
    return return_value