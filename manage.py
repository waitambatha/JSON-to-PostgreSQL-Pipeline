import datetime
from dateutil.relativedelta import relativedelta


def generate_last_six_months():
    # Get today's date and determine the first day of the current month
    today = datetime.date.today()
    selected_date = datetime.date(today.year, today.month, 1)

    # Start with the month immediately before the current month
    current_date = selected_date - relativedelta(months=1)
    results = []

    # Collect the last six months excluding the current month
    for _ in range(6):
        results.append(current_date.strftime("%Y%m"))
        current_date -= relativedelta(months=1)

    return ";".join(results)


if __name__ == "__main__":
    result = generate_last_six_months()
    print("Last six months (excluding the current month):")
    print(result)
