# coding: utf-8
import datetime
from itertools import islice
import warnings


def get_date_from_line(line):
    start_pos = 7
    end_pos = line.find(']', start_pos)
    time_string = line[start_pos:end_pos]
    date_time = datetime.datetime.strptime(time_string, '%H:%M:%S %d.%m.%Y')
    line_date = date_time.date()
    return line_date


class SmsLogFileUtil(object):
    @classmethod
    def is_record_line(cls, log_line: str):
        return log_line.startswith('#')

    @staticmethod
    def get_line_no_range_old(log_file_path, begin_date, end_date):
        warnings.warn("this is deprecated, use get_line_no_range instead", DeprecationWarning)
        begin_line_no = 0
        end_line_no = 0
        with open(log_file_path) as log_file:
            cur_line = 0
            for line in log_file:
                if not SmsLogFileUtil.is_record_line(line):
                    # some line is :
                    # check emergency
                    # so, just ignore it.
                    continue
                cur_line += 1
                start_pos = 7
                end_pos = line.find(']', start_pos)
                time_string = line[start_pos:end_pos]
                date_time = datetime.datetime.strptime(time_string, '%H:%M:%S %d.%m.%Y')
                line_date = date_time.date()
                if line_date >= end_date:
                    return begin_line_no, end_line_no

                if line_date >= begin_date:
                    begin_line_no = cur_line
                    break
            else:
                return begin_line_no, end_line_no

            for line in log_file:
                cur_line += 1
                start_pos = 7
                end_pos = line.find(']', start_pos)
                time_string = line[start_pos:end_pos]
                date_time = datetime.datetime.strptime(time_string, '%H:%M:%S %d.%m.%Y')
                if date_time.date() >= end_date:
                    end_line_no = cur_line
                    break
            else:
                end_line_no = cur_line + 1

        return begin_line_no, end_line_no

    @staticmethod
    def get_line_no_range(log_file_path, begin_date, end_date, max_line_no=1000):
        begin_line_no = 0
        end_line_no = 0
        with open(log_file_path) as log_file:
            cur_first_line_no = 1
            while True:
                next_n_lines = list(islice(log_file, max_line_no))
                if not next_n_lines:
                    return begin_line_no, end_line_no

                # if last line less then begin date, skip to next turn.
                cur_pos = -1
                cur_last_line = next_n_lines[-1]
                while (-1 * cur_pos) < len(next_n_lines):
                    cur_last_line = next_n_lines[cur_pos]
                    if SmsLogFileUtil.is_record_line(cur_last_line):
                        break
                    cur_pos -= 1

                line_date = get_date_from_line(cur_last_line)
                if line_date < begin_date:
                    cur_first_line_no = cur_first_line_no + len(next_n_lines)
                    continue

                # find first line greater or equal to begin_date
                for i in range(0, len(next_n_lines)):
                    cur_line = next_n_lines[i]
                    if not SmsLogFileUtil.is_record_line(cur_line):
                        continue
                    line_date = get_date_from_line(cur_line)
                    if line_date >= begin_date:
                        begin_line_no = cur_first_line_no + i
                        break

                # begin line must be found
                assert begin_line_no > 0

                # check if some line greater or equal to end_date,
                # if begin_line_no == end_line_no, then there is no line returned.
                for i in range(begin_line_no - 1, len(next_n_lines)):
                    cur_line = next_n_lines[i]
                    if not SmsLogFileUtil.is_record_line(cur_line):
                        continue
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
                next_n_lines = list(islice(log_file, max_line_no))
                if not next_n_lines:
                    break

                cur_last_line = next_n_lines[-1]
                cur_pos = -1
                while (-1 * cur_pos) < len(next_n_lines):
                    cur_last_line = next_n_lines[cur_pos]
                    if SmsLogFileUtil.is_record_line(cur_last_line):
                        break
                    cur_pos -= 1

                # if last line less than end_date, skip to next run
                line_date = get_date_from_line(cur_last_line)
                if line_date < end_date:
                    cur_first_line_no = cur_first_line_no + len(next_n_lines)
                    continue

                # find end_date
                for i in range(0, len(next_n_lines)):
                    cur_line = next_n_lines[i]
                    if not SmsLogFileUtil.is_record_line(cur_line):
                        continue
                    line_date = get_date_from_line(cur_line)
                    if line_date >= end_date:
                        end_line_no = cur_first_line_no + i
                        return begin_line_no, end_line_no
                else:
                    return begin_line_no, cur_first_line_no + len(next_n_lines)

        return begin_line_no, end_line_no
