def generate_in_date_range(start_date, end_date):
    def in_date_range(record):
        return start_date <= record.date <= end_date
    return in_date_range


def print_records(records):
    for r in records:
        print(r.log_record)