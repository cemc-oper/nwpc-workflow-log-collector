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
    begin_date: datetime.date = None,
    end_date: datetime.date = None,
    batch_line_no: int = 1000,
) -> (int, int):
    """
    Get line number range in [begin_date, end_date)

    Parameters
    ----------
    log_file_path: str
        log file path
    begin_date: datetime.date
        begin date, [begin_date, end_date)
    end_date: datetime.date
        end date, [begin_date, end_date)
    batch_line_no: int
        number of log lines in one read

    Returns
    -------
    int, int
        begin line number and end line number, [begin_number, end_number)
    """
    begin_line_no = 0
    end_line_no = -1
    with open(log_file_path) as log_file:
        cur_first_line_no = 1
        while True:
            next_n_lines = list(islice(log_file, batch_line_no))
            if not next_n_lines:
                return begin_line_no, end_line_no

            # if last line less then begin date, skip to next turn.
            cur_pos = -1
            cur_last_line = next_n_lines[cur_pos]
            # while (-1 * cur_pos) < len(next_n_lines):
            #     cur_last_line = next_n_lines[cur_pos]
            #     if cur_last_line[0] == '#':
            #         break
            #     cur_pos -= 1

            if begin_date is None:
                begin_line_no = cur_first_line_no
            else:
                line_date = get_date_from_line(cur_last_line)
                if line_date < begin_date:
                    cur_first_line_no = cur_first_line_no + len(next_n_lines)
                    continue

                # find first line greater or equal to begin_date
                for i in range(0, len(next_n_lines)):
                    cur_line = next_n_lines[i]
                    # if cur_line[0] != '#':
                    #     continue
                    line_date = get_date_from_line(cur_line)
                    if line_date >= begin_date:
                        begin_line_no = cur_first_line_no + i
                        break

            # begin line must be found
            assert begin_line_no >= 0

            if end_date is None:
                end_line_no = cur_first_line_no + len(next_n_lines)
                cur_first_line_no = end_line_no
                break
            else:
                # check if some line greater or equal to end_date,
                # if begin_line_no == end_line_no, then there is no line returned.
                for i in range(begin_line_no - 1, len(next_n_lines)):
                    cur_line = next_n_lines[i]
                    # if cur_line[0] != '#':
                    #     continue
                    line_date = get_date_from_line(cur_line)
                    if line_date >= end_date:
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
            if not next_n_lines:
                break

            cur_last_line = next_n_lines[-1]
            cur_pos = -1
            # while (-1 * cur_pos) < len(next_n_lines):
            #     cur_last_line = next_n_lines[cur_pos]
            #     if cur_last_line[0] == '#':
            #         break
            #     cur_pos -= 1

            if end_date is None:
                end_line_no = cur_first_line_no + len(next_n_lines)
                cur_first_line_no = end_line_no
                continue

            # if last line less than end_date, skip to next run
            line_date = get_date_from_line(cur_last_line)
            if line_date < end_date:
                cur_first_line_no = cur_first_line_no + len(next_n_lines)
                continue

            # find end_date
            for i in range(0, len(next_n_lines)):
                cur_line = next_n_lines[i]
                # if cur_line[0] != '#':
                #     continue
                line_date = get_date_from_line(cur_line)
                if line_date >= end_date:
                    end_line_no = cur_first_line_no + i
                    return begin_line_no, end_line_no
            else:
                return begin_line_no, cur_first_line_no + len(next_n_lines)

    return begin_line_no, end_line_no


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
            start_date.date(),
            end_date.date(),
        )
        if begin_line_no == 0 or end_line_no == 0:
            logger.info("line not found")
            return
        logger.info(f"Found line range: {begin_line_no}, {end_line_no}")

        logger.info(f"Skipping lines before {begin_line_no}...")
        progressbar_before = pyprind.ProgBar(begin_line_no)

        batch_number = 1000
        batch_count = int(begin_line_no/batch_number)
        remain_lines = begin_line_no % batch_number
        for i in range(0, batch_count):
            next_n_lines = list(islice(f, batch_number))
            progressbar_before.update(batch_number)

        for i in range(0, remain_lines):
            next(f)
            progressbar_before.update()

        prog = re.compile(f"{node_path}")

        logger.info(f"Reading lines between {begin_line_no} and {end_line_no}...")
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
