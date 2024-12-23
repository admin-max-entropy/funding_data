from fastapi import FastAPI
import src.read_data_utils
import src.config
app = FastAPI()
@app.get("/")
def root():
    return {"Hello": "World"}

@app.get("/series/{series_name}")
def query_series(series_name:str):
    fred_name_map = src.config.FRED_SERIES_NAME_MAP
    ofr_name_map = src.config.OFR_SERIES_NAME_MAP
    if series_name in fred_name_map:
        return src.read_data_utils.read_fred_related_table(fred_name_map[series_name])
    elif series_name in ofr_name_map:
        return src.read_data_utils.read_ofr_data_table()[f"FNYR{ofr_name_map[series_name]}"]

@app.get("/treasury_maturing/{sec_type}")
def query_treasury_series_maturing(sec_type: str):
    if sec_type in src.config.TREASURY_SEC_MAP:
        return src.read_data_utils.read_treasury_data(src.config.TREASURY_SEC_MAP[sec_type],
                                                      src.config.TREASURY_DATA_TYPE_MATURING)

@app.get("/treasury_outstanding/{sec_type}")
def query_treasury_series_outstanding(sec_type: str):
    if sec_type in src.config.TREASURY_SEC_MAP:
        return src.read_data_utils.read_treasury_data(src.config.TREASURY_SEC_MAP[sec_type],
                                                      src.config.TREASURY_DATA_TYPE_OUTSTANDING)

@app.get("/treasury_settle/{sec_type}")
def query_treasury_series_settlement(sec_type: str):
    if sec_type in src.config.TREASURY_SEC_MAP:
        return src.read_data_utils.read_treasury_data(src.config.TREASURY_SEC_MAP[sec_type],
                                                      src.config.TREASURY_DATA_TYPE_SETTLEMENT)
