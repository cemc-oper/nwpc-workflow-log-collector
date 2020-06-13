import datetime
import re
from itertools import islice

import pyprind
from loguru import logger
from nwpc_workflow_log_model.log_record.ecflow import EcflowLogParser


def get_date_from_line(line: str) -> datetime.date:
    """
    Examples
    --------
    >>> get_date_from_line("LOG:[12:34:57 1.3.2020]  active: /grapes_meso_3km_post")
    datetime.date(2020, 3, 1)
    """
    start_pos = 5
    end_pos = line.find("]", start_pos)
    time_string = line[start_pos:end_pos]
    date_time = datetime.datetime.strptime(time_string, "%H:%M:%S %d.%m.%Y")
    line_date = date_time.date()
    return line_date


def is_record_line(log_line: str) -> bool:
    return True


def get_line_no_range(
    log_file_path: str,
    start_date: datetime.date = None,
    stop_date: datetime.date = None,
    batch_line_no: int = 1000,
) -> (int, int):
    """
    Get line number range in [start_date, stop_date)

    Parameters
    ----------
    log_file_path: str
        log file path
    start_date: datetime.date
        begin date, [start_date, stop_date)
    stop_date: datetime.date
        end date, [start_date, stop_date)
    batch_line_no: int
        number of log lines in one read

    Returns
    -------
    int, int
        begin line number and end line number, [begin_number, end_number)
    """
    logger.info("counting total line number...")
    num_lines = sum(1 for line in open(log_file_path))
    logger.info("got total line number: {}", num_lines)

    progressbar = pyprind.ProgBar(num_lines)

    begin_line_no = 0
    end_line_no = -1
    with open(log_file_path) as log_file:
        logger.info("finding begin line number for start_date {}", start_date)
        cur_first_line_no = 1
        while True:
            next_n_lines = list(islice(log_file, batch_line_no))
            progressbar.update(batch_line_no)
            if not next_n_lines:
                logger.warning("not find start_date {}, return ({}, {})", start_date, begin_line_no, end_line_no)
                return begin_line_no, end_line_no

            # if last line less then begin date, skip to next turn.
            cur_pos = -1
            cur_last_line = next_n_lines[cur_pos]
            if start_date is None:
                begin_line_no = cur_first_line_no
            else:
                current_line_date = get_date_from_line(cur_last_line)
                if current_line_date < start_date:
                    cur_first_line_no = cur_first_line_no + len(next_n_lines)
                    continue

                # find first line greater or equal to start_date
                for i in range(0, len(next_n_lines)):
                    cur_line = next_n_lines[i]
                    current_line_date = get_date_from_line(cur_line)
                    if current_line_date >= start_date:
                        begin_line_no = cur_first_line_no + i
                        break

            # begin line must be found
            assert begin_line_no >= 0
            logger.info("found begin line number for start_date {}: {}", start_date, begin_line_no)

            logger.info("finding end line number for stop_date {}", stop_date)
            if stop_date is None:
                end_line_no = cur_first_line_no + len(next_n_lines)
                cur_first_line_no = end_line_no
                break
            else:
                # check if some line greater or equal to stop_date,
                # if begin_line_no == end_line_no, then there is no line returned.
                for i in range(cur_first_line_no - 1, len(next_n_lines)):
                    cur_line = next_n_lines[i]
                    current_line_date = get_date_from_line(cur_line)
                    if current_line_date >= stop_date:
                        end_line_no = cur_first_line_no + i
                        if begin_line_no == end_line_no:
                            begin_line_no = 0
                            end_line_no = 0
                        return begin_line_no, end_line_no
                cur_first_line_no = cur_first_line_no + len(next_n_lines)
                end_line_no = cur_first_line_no
                break

        while True:
            next_n_lines = list(islice(log_file, batch_line_no))
            progressbar.update(batch_line_no)
            if not next_n_lines:
                break

            cur_last_line = next_n_lines[-1]
            cur_pos = -1
            if stop_date is None:
                end_line_no = cur_first_line_no + len(next_n_lines)
                cur_first_line_no = end_line_no
                continue

            # if last line less than stop_date, skip to next run
            current_line_date = get_date_from_line(cur_last_line)
            if current_line_date < stop_date:
                cur_first_line_no = cur_first_line_no + len(next_n_lines)
                end_line_no = cur_first_line_no + len(next_n_lines)
                continue

            # find stop_date
            for i in range(0, len(next_n_lines)):
                cur_line = next_n_lines[i]
                current_line_date = get_date_from_line(cur_line)
                if current_line_date >= stop_date:
                    end_line_no = cur_first_line_no + i
                    logger.info("found end line number for stop_date {}: {}", stop_date, end_line_no)
                    return begin_line_no, end_line_no
            else:
                end_line_no = cur_first_line_no + len(next_n_lines)
                logger.info("found end line number for stop_date {}: {}", stop_date, end_line_no)
                return begin_line_no, end_line_no

    logger.info("found end line number for stop_date {}: {}", stop_date, end_line_no)
    return begin_line_no, end_line_no


def get_record_list(
        file_path: str,
        node_path: str,
        start_date: datetime.datetime,
        stop_date: datetime.datetime,
        show_progress_bar: bool = True,
):
    records = []
    with open(file_path) as f:
        logger.info(f"Finding line range in date range: {start_date}, {stop_date}")
        begin_line_no, end_line_no = get_line_no_range(
            file_path,
            start_date.date(),
            stop_date.date(),
        )
        if begin_line_no == 0 or end_line_no == 0:
            logger.info("line not found")
            return
        logger.info(f"Found line range: {begin_line_no}, {end_line_no}")

        logger.info(f"Skipping lines before {begin_line_no}...")
        if show_progress_bar:
            progressbar_before = pyprind.ProgBar(begin_line_no)

        batch_number = 1000
        batch_count = int(begin_line_no/batch_number)
        remain_lines = begin_line_no % batch_number
        for i in range(0, batch_count):
            next_n_lines = list(islice(f, batch_number))
            if show_progress_bar:
                progressbar_before.update(batch_number)

        for i in range(0, remain_lines-1):
            next(f)
            if show_progress_bar:
                progressbar_before.update()

        prog = re.compile(f"{node_path}")

        logger.info(f"Reading lines between {begin_line_no} and {end_line_no}...")
        if show_progress_bar:
            progressbar_read = pyprind.ProgBar(end_line_no - begin_line_no)
        for i in range(begin_line_no, end_line_no):
            progressbar_read.update()
            line = f.readline()
            line = line.strip()

            result = prog.search(line)
            if result is None:
                continue

            parser = EcflowLogParser()
            record = parser.parse(line)
            records.append(record)

    return records
