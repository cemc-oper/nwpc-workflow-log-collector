from pathlib import Path

import yaml


def load_config(config_file_path: str or Path):
    f = open(config_file_path, "r")
    config = yaml.safe_load(f)
    f.close()
    return config
