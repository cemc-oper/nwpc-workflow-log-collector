import datetime
import typing

import pandas as pd
from loguru import logger
from scipy import stats

from nwpc_workflow_model.node_status import NodeStatus

from nwpc_workflow_log_model.log_record.ecflow import StatusLogRecord
from nwpc_workflow_log_model.log_record.ecflow.status_record import StatusChangeEntry
from nwpc_workflow_log_model.analytics.family_status_change_dfa import (
    FamilyStatusChangeDFA,
    FamilySituationType,
)

from nwpc_workflow_log_collector.ecflow.log_file_util import get_record_list
from nwpc_workflow_log_collector.ecflow.util import generate_in_date_range, print_records


def analytics_family_node_log_with_status(
        file_path: str,
        node_path: str,
        node_status: NodeStatus,
        start_date: datetime.datetime,
        stop_date: datetime.datetime,
        verbose: int,
):
    logger.info(f"Analytic time points for family node")
    logger.info(f"\tnode_path: {node_path}")
    logger.info(f"\tnode_status: {node_status}")
    logger.info(f"\tstart_date: {start_date}")
    logger.info(f"\tstop_date: {stop_date}")

    logger.info(f"Getting log lines...")
    records = get_record_list(file_path, node_path, start_date, stop_date)
    logger.info(f"Getting log lines...Done, {len(records)} lines")

    situations = get_family_node_situations(
        records=records,
        node_path=node_path,
        start_date=start_date,
        end_date=stop_date,
    )

    calculate_for_family_node_status(
        situations=situations,
        node_status=node_status
    )


def get_family_node_situations(
        records: list,
        node_path: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
) -> typing.List:
    logger.info("Finding StatusLogRecord for {}", node_path)
    record_list = []
    for record in records:
        if record.node_path == node_path and isinstance(record, StatusLogRecord):
            record_list.append(record)

    logger.info("Calculating family node status change using DFA...")
    situations = []
    for current_date in pd.date_range(start=start_date, end=end_date, closed="left"):
        filter_function = generate_in_date_range(current_date, current_date + pd.Timedelta(days=1))
        current_records = list(filter(lambda x: filter_function(x), record_list))

        status_changes = [StatusChangeEntry(r) for r in current_records]

        dfa = FamilyStatusChangeDFA(
            name=current_date,
            ignore_aborted=True,
        )

        for s in status_changes:
            dfa.trigger(
                s.status.value,
                node_data=s,
            )
            if dfa.state in (
                FamilySituationType.Complete,
                FamilySituationType.Error,
            ):
                break

        situations.append({
            "date": current_date,
            "state": dfa.state,
            "situation": dfa.node_situation,
            "records": current_records,
        })

    logger.info("Calculating family node status change using DFA...Done")
    return situations


def calculate_for_family_node_status(
        situations: typing.List,
        node_status: NodeStatus,
):
    time_series = []
    for a_situation in situations:
        current_date = a_situation["date"]
        current_records = a_situation["records"]
        if a_situation["state"] is FamilySituationType.Complete:
            node_situation = a_situation["situation"]
            time_points = node_situation.time_points
            point = next((i for i in time_points if i.status == node_status), None)
            if point is None:
                logger.warning("[{}] skip: no time point {}", current_date.strftime("%Y-%m-%d"), node_status)
                # print_records(current_records)
            else:
                time_length = point.time - current_date
                time_series.append(time_length)
                logger.info("[{}] {}", current_date.strftime("%Y-%m-%d"), time_length)
        else:
            logger.warning("[{}] skip: DFA is not in complete", current_date.strftime("%Y-%m-%d"))
            # print_records(current_records)

    time_series = pd.Series(time_series)
    time_series_mean = time_series.mean()
    print()
    print("Mean:")
    print(time_series_mean)

    ratio = 0.25
    time_series_trim_mean = stats.trim_mean(time_series.values, ratio)
    print()
    print(f"Trim Mean ({ratio}):")
    print(pd.to_timedelta(time_series_trim_mean))
