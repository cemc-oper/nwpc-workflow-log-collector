from typing import List

from nwpc_workflow_log_model.log_record.log_record import LogRecord


def generate_in_date_range(start_date, end_date):
    def in_date_range(record):
        return start_date <= record.date <= end_date
    return in_date_range


def print_records(records: List[LogRecord]):
    for r in records:
        print(r.log_record)
