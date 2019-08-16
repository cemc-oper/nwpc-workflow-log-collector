# coding: utf-8
import yaml


def load_config(config_file_path):
    f = open(config_file_path, "r")
    config = yaml.safe_load(f)
    f.close()
    return config
