import pandas
import functools
import src.config
import src.data_utils
from src.data_utils import create_fedfund_volume_decomposition_table


@functools.lru_cache(maxsize=1024)
def store_fedfund_volume_decomposition():
    """
    :return: dictionary of fedfund volume
    """
    year = "2024"
    quarters=["q4", "q3", "q2"]

    data = []
    for quarter in quarters:
        try:
            link = ("https://www.newyorkfed.org/medialibrary/media/markets/rate-revisions"
                    f"/{year}/FR2420-summary-statistics-{quarter}{year}.xlsx")
            data = pandas.read_excel(link, sheet_name="Effective Federal Funds Rate")
            data = data.to_dict(orient="records")

        except ValueError as e:
            print(str(e))

        if len(data) > 0:
            break

    sql = (f"INSERT IGNORE INTO {src.config.TABLE_FF_DECOMP_VOLUME}"
           f"(date, total_volume, fbo_volume, domestic_bank_volume) VALUES (%s, %s, %s, %s)")

    db = src.data_utils.get_database(src.config.DATABASE_STIR)
    cursor = db.cursor()

    for row in data:
        val = (row["Date"], row["Total Volume "], row["FBO Volume"], row["Domestic Bank Volume"])
        cursor.execute(sql, val)

    db.commit()

create_fedfund_volume_decomposition_table()
store_fedfund_volume_decomposition()