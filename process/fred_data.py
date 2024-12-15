import src.data_utils
import src.config
from datetime import datetime

end_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
for table_name in [
                    src.config.TABLE_EFFR, src.config.TABLE_RRP_VOLUME,
                    src.config.TABLE_RRP_RATE, src.config.TABLE_FOREIGN_RRP,
                    src.config.TABLE_LOWER_BOUND, src.config.TABLE_UPPER_BOUND,
                    src.config.TABLE_RESERVE_BALANCE,
                    src.config.TABLE_IORB
                   ]:
    src.data_utils.drop_fred_related_table(table_name)
    src.data_utils.create_fred_related_table(table_name)
    start_date = src.config.FRED_RT_START_DATE
    src.data_utils.store_fred_related_table(table_name, start_date, end_date)

table_name = src.config.TABLE_IORB
src.data_utils.prepend_iorb_table()