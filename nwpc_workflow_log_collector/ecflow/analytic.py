import datetime
import re
from itertools import islice

from loguru import logger
from nwpc_workflow_log_model.log_record.ecflow import EcflowLogRecord, EcflowLogParser, StatusLogRecord
from nwpc_workflow_model.node_status import NodeStatus
import pyprind
import pandas as pd
from scipy import stats

from .log_file_util import get_line_no_range


def analytics_log_from_local_file(
        file_path: str,
        node_path: str,
        node_status: NodeStatus.submitted,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        verbose: int,
):
    with open(file_path) as f:
        logger.info(f"Analytic time points")
        logger.info(f"\tnode_path: {node_path}")
        logger.info(f"\tnode_status: {node_status}")
        logger.info(f"\tstart_date: {start_date}")
        logger.info(f"\tend_date: {end_date}")

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

        record_list = []

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
