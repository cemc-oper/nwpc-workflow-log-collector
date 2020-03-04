import datetime
import re
from itertools import islice

from loguru import logger
import pyprind
import pandas as pd
from scipy import stats

from nwpc_workflow_log_model.log_record.ecflow import EcflowLogParser, StatusLogRecord
from nwpc_workflow_log_model.log_record.ecflow.status_record import StatusChangeEntry
from nwpc_workflow_log_model.analytics.node_situation import (
    SituationType,
    NodeStatus,
)
from nwpc_workflow_log_model.analytics.node_status_change_dfa import NodeStatusChangeDFA

from .log_file_util import get_line_no_range


def analytics_node_log_with_status(
        file_path: str,
        node_path: str,
        node_status: NodeStatus,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        verbose: int,
):
    logger.info(f"Analytic time points")
    logger.info(f"\tnode_path: {node_path}")
    logger.info(f"\tnode_status: {node_status}")
    logger.info(f"\tstart_date: {start_date}")
    logger.info(f"\tend_date: {end_date}")

    logger.info(f"Getting log lines...")
    records = get_record_list(file_path, node_path, start_date, end_date)
    logger.info(f"Getting log lines...Done, {len(records)} lines")

    analytic_status_point_dfa(records, node_path, node_status, start_date, end_date)


def get_record_list(
        file_path: str,
        node_path: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
):
    records = []
    with open(file_path) as f:
        logger.info(f"Finding line range in date range: {start_date}, {end_date}")
        begin_line_no, end_line_no = get_line_no_range(
            file_path,
            datetime.datetime.strptime(start_date, "%Y-%m-%d").date(),
            datetime.datetime.strptime(end_date, "%Y-%m-%d").date(),
        )
        if begin_line_no == 0 or end_line_no == 0:
            logger.info("line not found")
            return
        logger.info(f"Found line range: {begin_line_no}, {end_line_no}")

        logger.info(f"Skipping lines before {begin_line_no}...")
        pbar_before = pyprind.ProgBar(begin_line_no)

        batch_number = 1000
        batch_count = int(begin_line_no/batch_number)
        remain_lines = begin_line_no % batch_number
        for i in range(0, batch_count):
            next_n_lines = list(islice(f, batch_number))
            pbar_before.update(batch_number)

        for i in range(0, remain_lines):
            next(f)
            pbar_before.update()

        prog = re.compile(f"{node_path}")

        logger.info(f"Reading lines between {begin_line_no} and {end_line_no}")
        pbar_read = pyprind.ProgBar(end_line_no - begin_line_no)
        for i in range(begin_line_no, end_line_no):
            pbar_read.update()
            line = f.readline()
            line = line.strip()

            result = prog.search(line)
            if result is None:
                continue

            parser = EcflowLogParser()
            record = parser.parse(line)
            records.append(record)

    return records


def analytic_status_point(
        records: list,
        node_path: str,
        node_status: NodeStatus,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
):
    record_list = []
    for record in records:
        if record.node_path == node_path and isinstance(record, StatusLogRecord):
            if record.status == node_status:
                record_list.append(record)

    print(f"Time series for {node_path} with {node_status}:")
    for r in record_list:
        print(f"{r.date} {r.time}")

    time_series = pd.Series([
        (datetime.datetime.combine(r.date, r.time) - datetime.datetime.combine(r.date, datetime.time.min))
        for r in record_list
    ])
    print("Mean:")
    print(time_series.mean())

    trim_mean = stats.trim_mean(time_series.values, 0.25)
    print("Trim Mean (0.25):")
    print(pd.to_timedelta(trim_mean))


def analytic_status_point_dfa(
        records: list,
        node_path: str,
        node_status: NodeStatus,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
):
    record_list = []
    for record in records:
        if record.node_path == node_path and isinstance(record, StatusLogRecord):
            record_list.append(record)

    time_series = []
    for current_date in pd.date_range(start=start_date, end=end_date, closed="left"):
        filter_function = generate_in_date_range(current_date, current_date + pd.Timedelta(days=1))
        current_records = list(filter(lambda x: filter_function(x), record_list))

        status_changes = [StatusChangeEntry(r) for r in current_records]

        dfa = NodeStatusChangeDFA(name=current_date)

        for s in status_changes:
            dfa.trigger(
                s.status.value,
                node_data=s,
            )
            if dfa.state is SituationType.Complete:
                break

        if dfa.state is SituationType.Complete:
            node_situation = dfa.node_situation
            p = node_situation.time_points[1]
            if p.status != NodeStatus.submitted:
                logger.warning("[{}] skip: there is no submitted", current_date.strftime("%Y-%m-%d"))
                print_records(current_records)
            else:
                time_length = p.time - current_date
                time_series.append(time_length)
                logger.info("[{}] {}", current_date.strftime("%Y-%m-%d"), time_length)
        else:
            logger.warning("[{}] skip: DFA is not in complete", current_date.strftime("%Y-%m-%d"))
            print_records(current_records)

    time_series = pd.Series(time_series)
    print("Mean:")
    print(time_series.mean())

    trim_mean = stats.trim_mean(time_series.values, 0.25)
    print("Trim Mean (0.25):")
    print(pd.to_timedelta(trim_mean))


def generate_in_date_range(start_date, end_date):
    def in_date_range(record):
        return start_date <= record.date <= end_date
    return in_date_range


def print_records(records):
    for r in records:
        print(r.log_record)
