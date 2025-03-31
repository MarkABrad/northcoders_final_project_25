from datetime import datetime

# create a function that will retreieve the day from the year/month/day of the inpute date from dim_date
# day will be given in interger, so write another functon to convert it to a day in string
# the return all the following info
# (date_id,year,month,day,day_of_week,day_name,month_name,quarter)


def extract_date_info_from_dim_date(date_id):
    try:
        date_obj = datetime.strptime(date_id, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        date_obj = datetime.strptime(date_id, "%Y-%m-%d")

    date_id = date_obj.strftime("%Y-%m-%d")
    year = date_obj.year
    month = date_obj.month
    day = date_obj.day
    day_of_week = date_obj.weekday()
    day_of_week_name = date_obj.strftime("%A")
    month_name = date_obj.strftime("%B")
    quarter = (month - 1) // 3 + 1

    return {
        "date_id": date_id,
        "year": year,
        "month": month,
        "day": day,
        "day_of_week": day_of_week,
        "day_name": day_of_week_name,
        "month_name": month_name,
        "quarter": quarter,
    }
