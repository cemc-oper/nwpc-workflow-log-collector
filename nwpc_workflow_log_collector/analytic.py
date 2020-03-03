import json
import datetime

import click

from nwpc_workflow_log_collector.ecflow.analytic import analytics_node_log_with_status
from nwpc_workflow_model.node_status import NodeStatus


@click.group()
def cli():
    pass


@cli.command("node")
@click.option("-l", "--log-file", help="log file path")
@click.option("-n", "--node-path", help="node path")
@click.option("-s", "--node-status", default="submitted", help="node path")
@click.option("--begin-date", default=None, help="begin date, date range: [begin_date, end_date), YYYY-MM-dd")
@click.option("--end-date", default=None, help="end date, date range: [begin_date, end_date), YYYY-MM-dd")
@click.option("-v", "--verbose", count=True, help="verbose level")
def load_range(
    log_file, node_path, node_status, begin_date, end_date, verbose
):
    analytics_node_log_with_status(
        log_file,
        node_path,
        NodeStatus[node_status],
        begin_date,
        end_date,
        verbose,
    )


if __name__ == "__main__":
    cli()
