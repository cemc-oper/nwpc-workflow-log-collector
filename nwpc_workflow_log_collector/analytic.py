import datetime

import click

from nwpc_workflow_log_collector.ecflow.analytic import (
    analytics_task_node_log_with_status,
    analytics_family_node_log_with_status,
)
from nwpc_workflow_model.node_status import NodeStatus


@click.group()
def cli():
    pass


@cli.command("node")
@click.option("-l", "--log-file", help="log file path")
@click.option("-n", "--node-path", required=True, help="node path")
@click.option(
    "-s", "--node-status",
    default=NodeStatus.submitted.value,
    type=click.Choice([s.value for s in [
        NodeStatus.submitted,
        NodeStatus.queued,
        NodeStatus.active,
        NodeStatus.aborted,
        NodeStatus.complete,
    ]]),
    help="node status",
)
@click.option("--node-type", default="task", type=click.Choice(["task", "family"]), help="node type")
@click.option("--start-date", default=None, help="start date, date range: [start_date, stop_date), YYYY-MM-dd")
@click.option("--stop-date", default=None, help="stop date, date range: [start_date, stop_date), YYYY-MM-dd")
@click.option("-v", "--verbose", count=True, help="verbose level")
def analytics_node(
        log_file: str,
        node_path: str,
        node_type: str,
        node_status: str,
        start_date: str,
        stop_date: str,
        verbose: int
):
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    stop_date = datetime.datetime.strptime(stop_date, "%Y-%m-%d")
    node_status = NodeStatus[node_status]

    if node_type == "task":
        analytics_task_node_log_with_status(
            log_file,
            node_path,
            node_status,
            start_date,
            stop_date,
            verbose,
        )
    elif node_type == "family":
        analytics_family_node_log_with_status(
            log_file,
            node_path,
            node_status,
            start_date,
            stop_date,
            verbose,
        )


if __name__ == "__main__":
    cli()
