from pathlib import Path

import yaml


def load_config(config_file_path: str or Path):
    with open(config_file_path, "r") as f:
        config = yaml.safe_load(f)
        return config
