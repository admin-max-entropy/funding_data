from datetime import datetime

import entropy.date_utils

result = entropy.date_utils.is_last_calendar_date_of_month(datetime(2023, 1, 1))
print(result)
