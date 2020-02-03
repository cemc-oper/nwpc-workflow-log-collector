from nwpc_workflow_log_collector import sms as sms_collector, ecflow as ecflow_collector
from nwpc_workflow_log_model.rmdb.ecflow.record import EcflowRecord
from nwpc_workflow_log_model.rmdb.sms.record import SmsRecord


def get_record_class(workflow_type: str):
    if workflow_type == "sms":
        return SmsRecord
    elif workflow_type == "ecflow":
        return EcflowRecord
    else:
        raise ValueError(f"workflow type is not supported: {workflow_type}")


def get_collector_module(workflow_type: str):
    if workflow_type == "sms":
        return sms_collector
    elif workflow_type == "ecflow":
        return ecflow_collector
    else:
        raise ValueError(f"workflow type is not supported: {workflow_type}")
