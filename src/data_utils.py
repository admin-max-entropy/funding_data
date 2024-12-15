from datetime import datetime
import src.config
import mysql.connector
import requests
import pandas

def create_database(database_name):
    mydb = mysql.connector.connect(
        host=src.config.HOST,
        user=src.config.USER,
        passwd=src.config.PWD,
    )
    cursor = mydb.cursor()
    cursor.execute(f'CREATE DATABASE {database_name}')
    cursor.execute("SHOW DATABASES")


def get_database(database_name):
    return mysql.connector.connect(
        host=src.config.HOST,
        user=src.config.USER,
        passwd=src.config.PWD,
        database=database_name
    )


def create_fedfund_volume_decomposition_table():
    db = get_database(src.config.DATABASE_STIR)
    cursor = db.cursor()
    table_name = src.config.TABLE_FF_DECOMP_VOLUME
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (date DATETIME, total_volume FLOAT, "
                   f"fbo_volume FLOAT, domestic_bank_volume FLOAT,"
                   f"PRIMARY KEY (date))")


def create_fred_related_table(table_name):
    db = get_database(src.config.DATABASE_STIR)
    cursor = db.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (date DATETIME, value FLOAT, "
                   f"PRIMARY KEY (date))")


def drop_fred_related_table(table_name):
    db = get_database(src.config.DATABASE_STIR)
    cursor = db.cursor()
    cursor.execute(f"DROP TABLE {table_name}")

def __query_format(series_id: str, start_date: datetime, end_date: datetime):
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")
    path = (f"https://api.stlouisfed.org/fred/series"
            f"/observations?series_id={series_id}&api_key={src.config.FRED_API_KEY}"
            f"&file_type=json&realtime_start={start_date}&realtime_end={end_date}")
    return path

def prepend_iorb_table():

    data_key = "IOER"
    end_date = datetime(2021, 7, 28)
    start_date = datetime(2016, 1, 1)
    data_path = __query_format(data_key, start_date, end_date)
    iorb_data = requests.get(data_path, timeout=60).json()
    table_name = src.config.TABLE_IORB

    sql = (f"INSERT {table_name} "
           f"(date, value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE value=values(value)")

    db = get_database(src.config.DATABASE_STIR)
    cursor = db.cursor()

    for row in iorb_data["observations"]:
        if row["value"] == ".":
            continue
        date = datetime.strptime(row["date"], "%Y-%m-%d")
        value = float(row["value"])
        val = (date, value)
        print(val)
        cursor.execute(sql, val)

    db.commit()

def store_fred_related_table(table_name:str, start_date: datetime, end_date:datetime):

    data_key = src.config.FRED_DATA_MAP[table_name]
    data_path = __query_format(data_key, start_date, end_date)
    iorb_data = requests.get(data_path, timeout=60).json()

    sql = (f"INSERT IGNORE INTO {table_name}"
           f"(date, value) VALUES (%s, %s)")

    db = get_database(src.config.DATABASE_STIR)
    cursor = db.cursor()

    for row in iorb_data["observations"]:
        if row["value"] == ".":
            continue
        date = datetime.strptime(row["date"], "%Y-%m-%d")
        value = float(row["value"])
        val = (date, value)
        print(val)
        cursor.execute(sql, val)

    db.commit()

def update_tga_balance_data():

    table_name = src.config.TABLE_TGA_BALANCE
    create_fred_related_table(table_name)

    sql = (f"INSERT {table_name} "
           f"(date, value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE value=values(value)")

    db = get_database(src.config.DATABASE_STIR)
    cursor = db.cursor()

    for key in ["Treasury General Account (TGA)", "Treasury General Account (TGA) Opening Balance",
                "Federal Reserve Account"]:
        link = (
            "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting"
            "/dts/operating_cash_balance?fields=record_date,account_type,"
            f"open_today_bal&filter=record_date:gte:2017-01-01,account_type:eq:{key}&page[size]=10000")
        data = requests.get(link, timeout=60).json()
        for row in data["data"]:
            date = datetime.strptime(row["record_date"], "%Y-%m-%d")
            if row["open_today_bal"] == "null":
                continue
            value = float(row["open_today_bal"])
            val = (date, value)
            print(val)
            cursor.execute(sql, val)

    db.commit()

def update_ofr_on_data():
    url = "https://data.financialresearch.gov/v1/series/dataset?dataset=fnyr"
    data = requests.get(url, timeout=60)
    data_set = data.json()
    result = {}
    preset_keys = []
    for key in ["BGCR", "EFFR", "OBFR", "SOFR", "TGCR"]:
        preset_keys += [f"FNYR-{key}-A"]
        for key_ in ["1Pctl", "25Pctl", "75Pctl", "99Pctl", "UV"]:
            preset_keys += [f"FNYR-{key}_{key_}-A"]

    for key, data in data_set["timeseries"].items():
        timeseries = data["timeseries"]["aggregation"]
        for row in timeseries:
            date = datetime.strptime(row[0], "%Y-%m-%d")
            if date not in result:
                result[date] = {}
            result[date][key] = float(row[1]) if row[1] is not None else None
    for date in result:
        for key in preset_keys:
            if key not in result[date]:
                result[date][key] = None

    table_name = src.config.TABLE_OFR_ON_DATA
    drop_fred_related_table(table_name)

    table_key_string = ""
    value_string = ""
    key_string = ""

    for key in list(result.values())[0]:
        table_key_string += f"{key.replace('-', '')} FLOAT, "
        value_string += "%s, "
        key_string += f"{key.replace('-', '')}, "

    value_string = value_string[:-2]
    key_string = key_string[:-2]

    db = get_database(src.config.DATABASE_STIR)
    cursor = db.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} "
                   f"(date DATETIME, {table_key_string[:-1]} "
                   f"PRIMARY KEY (date))")

    sql = (f"INSERT IGNORE INTO {table_name}"
           f"(date, {key_string}) VALUES (%s, {value_string})")

    for date, value in result.items():
        if date < src.config.OFR_DATE_START_DATE:
            continue
        values = list(value.values())
        val = tuple([date]+values)
        print(val)
        cursor.execute(sql, val)

    db.commit()

def daylight_overdraft_data():
    link = "https://www.federalreserve.gov/paymentsystems/files/psr_dlod.txt"
    data = requests.get(link, timeout=10)
    data = data.text.split("\n")

    table_name = src.config.TABLE_DAYLIGHT_OVERDFRAT
    drop_fred_related_table(table_name)

    columns_mapping = {2: "Peak_Total", 3: "Peak_Funds", 4: "Peak_Book_Entry",
                       5: "Average_Total", 6: "Average_Funds", 7: "Average_Book_Entry",
                       8: "Peak_Collateralized", 10: "Average_Collateralized",
                       }
    table_key_string = ""
    key_string = ""
    value_string = ""

    for index in columns_mapping:
        table_key_string += f"{columns_mapping[index]} FLOAT, "
        key_string += f"{columns_mapping[index]}, "
        value_string += "%s, "

    value_string = value_string[:-2]
    key_string = key_string[:-2]

    db = get_database(src.config.DATABASE_STIR)
    cursor = db.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} "
                   f"(date DATETIME, {table_key_string[:-1]} "
                   f"PRIMARY KEY (date))")

    sql = (f"INSERT IGNORE INTO {table_name}"
           f"(date, {key_string}) VALUES (%s, {value_string})")

    start_row = 9
    for inx in range(start_row, len(data)):
        row_info = data[inx]
        dummy = ' '.join(row_info.split())
        dummy = dummy.split(" ")
        if len(dummy) != 12:
            break
        values = []
        date = datetime.strptime(dummy[0], "%m/%d/%Y")
        for inx_, key in columns_mapping.items():
            values += [float(dummy[inx_].replace(",", "").replace("$", ""))]
        val = tuple([date] + values)
        print(val)
        cursor.execute(sql, val)
    db.commit()


def elasticity_data():
    data_link = ("https://www.newyorkfed.org/medialibrary/Research/Interactives"
                 "/Data/elasticity/download-data")
    data = pandas.read_excel(data_link, sheet_name="chart data", skiprows=3, header=1)
    data = data.to_dict(orient="records")
    data_result = {}

    data_keys = ["2.5th", "16th", "50th", "84th", "97.5th"]
    table_name = src.config.TABLE_ELASTICITY
    drop_fred_related_table(table_name)

    db = get_database(src.config.DATABASE_STIR)
    cursor = db.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} "
                   f"(date DATETIME, 2_5th FLOAT, 16th FLOAT, 50th FLOAT, 84th FLOAT, 97_5th FLOAT, "
                   f"PRIMARY KEY (date))")

    sql = (f"INSERT IGNORE INTO {table_name}"
           f"(date, 2_5th, 16th, 50th, 84th, 97_5th) VALUES (%s, %s, %s, %s, %s, %s)")

    for row in data:

        date = row["Date"]
        if date not in data_result:
            data_result[date] = {}

        for key in data_keys:
            m_key = f"Elasticity - {key} percentile"
            if "50th" in key:
                m_key += " (main)"
            data_result[date][key] = row[m_key]

        val = tuple([date] + list(data_result[date].values()))
        print(val)
        cursor.execute(sql, val)

    db.commit()