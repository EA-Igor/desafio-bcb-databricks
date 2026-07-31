# Databricks notebook source
import os
import sys


def add_src_to_path() -> None:
    candidates = [os.getcwd(), os.path.dirname(os.getcwd())]
    for candidate in candidates:
        src_path = os.path.join(candidate, "src")
        package_path = os.path.join(src_path, "desafio_bcb")
        if os.path.isdir(package_path) and src_path not in sys.path:
            sys.path.insert(0, src_path)
            return


add_src_to_path()

from desafio_bcb.bronze import run_bronze
from desafio_bcb.notebook_utils import config_from_widgets


run_bronze(spark, config_from_widgets(dbutils))

