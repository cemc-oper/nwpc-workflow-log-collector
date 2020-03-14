from datetime import datetime
import pandas as pd

from nwpc_workflow_log_model.log_record.ecflow import EcflowLogParser, StatusLogRecord
from nwpc_workflow_log_model.log_record.ecflow.status_record import StatusChangeEntry
from nwpc_workflow_log_model.analytics.situation_type import FamilySituationType
from nwpc_workflow_log_model.analytics.family_status_change_dfa import FamilyStatusChangeDFA, NodeStatus


def test_family_node():
    log_file = "./dist/log/grapes_gfs/grapes_gfs_post_start_hour.txt"
    begin_date = datetime(2020, 3, 1)
    end_date = datetime(2020, 3, 10)

    records = []
    with open(log_file) as f:
        for line in f:
            line = line.strip()

            parser = EcflowLogParser()
            record = parser.parse(line)
            if isinstance(record, StatusLogRecord) and record.node_path == "/gmf_grapes_gfs_post/00":
                records.append(record)

    for current_date in pd.date_range(start=begin_date, end=end_date, closed="left"):
        filter_function = generate_in_date_range(current_date, current_date + pd.Timedelta(days=1))
        current_records = list(filter(lambda x: filter_function(x), records))
        print(current_date)

        status_changes = [StatusChangeEntry(r) for r in current_records]

        dfa = FamilyStatusChangeDFA(name=current_date)

        for s in status_changes:
            dfa.trigger(
                s.status.value,
                node_data=s,
            )
            if dfa.state in (
                FamilySituationType.Complete,
                FamilySituationType.Unknown,
                FamilySituationType.Error,
            ):
                break

        if dfa.state is FamilySituationType.Complete:
            node_situation = dfa.node_situation
            p = node_situation.time_points[-1]
            if p.status != NodeStatus.complete:
                print("skip, there is no submitted")
            else:
                time_length = p.time - current_date
                print(time_length)
        else:
            print(f"skip, DFA is not in complete: {dfa.state.value}")


def generate_in_date_range(start_date, end_date):
    def in_date_range(record):
        return start_date <= record.date <= end_date
    return in_date_range


if __name__ == "__main__":
    test_family_node()
