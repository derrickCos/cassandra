#!/usr/bin/env python3

import os
import time
import signal
import subprocess

from argparse import ArgumentParser
from collections import OrderedDict
from dataclasses import dataclass
from typing import Optional, List


@dataclass
class NoSQLBenchConfig:
    host: str
    localdc: str
    tags: str
    alias: str
    cycles: int
    threads: int
    errors: Optional[str]
    workload_args: Optional[dict]
    labels: Optional[dict]


home_dir = os.path.join("./")
artifact_dir = os.path.join(home_dir, "./artifacts")
library_dir = os.path.join(home_dir, "./library")
testdata_dir = os.path.join(home_dir, "./testdata")
scratch_dir = os.path.join(home_dir, "./scratch")

dimension_map = {
    "glove-25": 25,
    "glove-50": 50,
    "glove-100": 100,
    "glove-200": 200,
    "last.fm": 65,
    "deep1b": 96
}

hdf5_path_map = {
    "glove-25": os.path.join(testdata_dir, "glove-25-angular.hdf5"),
    "glove-50": os.path.join(testdata_dir, "glove-50-angular.hdf5"),
    "glove-100": os.path.join(testdata_dir, "glove-100-angular.hdf5"),
    "glove-200": os.path.join(testdata_dir, "glove-200-angular.hdf5"),
    "last.fm": os.path.join(testdata_dir, "lastfm-64-dot.hdf5"),
    "deep1b": os.path.join(testdata_dir, "deep-image-96-angular.hdf5")
}

similarity_function_map = {
    "glove-25": "cosine",
    "glove-50": "cosine",
    "glove-100": "cosine",
    "glove-200": "cosine",
    "last.fm": "dot_product",
    "deep1b": "cosine"
}

training_cycles_map = {
    "glove-25": 1183514,
    "glove-50": 1183514,
    "glove-100": 1183514,
    "glove-200": 1183514,
    "last.fm": 292385,
    "deep1b": 9990000
}

test_cycles_map = {
    "glove-25": 10000,
    "glove-50": 10000,
    "glove-100": 10000,
    "glove-200": 10000,
    "last.fm": 10000,
    "deep1b": 10000
}

nb5_path = os.path.join(library_dir, "./nb5.jar")
yaml_path = os.path.join(library_dir, "ann_benchmarks.yaml")
username = "cassandra"
password = "cassandra"


def create_output_dir(dataset: str) -> str:
    subdir = os.path.join(artifact_dir, dataset + "_nb5_logs")
    os.makedirs(subdir, exist_ok=True)
    return subdir


def get_label_map(dataset: str, workload_configs: dict) -> dict:
    label_map = OrderedDict()
    label_map["dataset"] = dataset
    if "M" in workload_configs:
        label_map["M"] = str(workload_configs["M"])
    if "ef" in workload_configs:
        label_map["ef"] = str(workload_configs["ef"])
    return label_map

def run_nosqlbench_cmd(config: NoSQLBenchConfig, output_dir: Optional[str] = None):
    cmd = []
    cmd.append("java")
    cmd.append("-jar")
    cmd.append(nb5_path)
    cmd.append("start")
    cmd.append(f"host={config.host}")
    cmd.append(f"localdc={config.localdc}")
    cmd.append(f"alias={config.alias}")
    cmd.append(f"workload={yaml_path}")
    cycle_range = f"0..{config.cycles}"
    cmd.append(f"cycles={cycle_range}")
    if config.errors:
        cmd.append(f"errors={config.errors}")
    for k, v in config.workload_args.items():
        cmd.append(f"{k}={v}")
    cmd.append("driver=cql")
    cmd.append(f"tags={config.tags}")
    cmd.append(f"threads={config.threads}")
    cmd.append("pooling=16:16:1024")
    labels = []
    for k, v in config.labels.items():
        labels.append(f"{k}={v}")
    labels = ",".join(labels)
    cmd.append("--add-labels")
    cmd.append(f"{labels}")
    cmd.append(f"--report-csv-to={output_dir}/{config.alias}.csv")
    cmd.append(f"--report-interval=1")
    cmd.append("-v")
    cmd.append("--show-stacktraces")
    output_file_name = []
    for k, v in config.labels.items():
        output_file_name.append(v)
    output_file_name.append(config.alias)
    output_file_name = "_".join(output_file_name)
    output_file_name += ".log"
    nb_output_path = os.path.join(output_dir, output_file_name)

    cmd_str = " ".join(cmd)
    print(f"cmd: {cmd_str}")
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, universal_newlines=True) as proc:
        try:
            for line in iter(proc.stdout.readline, ''):
                #print(line)
                with open(nb_output_path, "a") as log_file:
                    log_file.write(line)
                if "ExecutionResult -- SCENARIO TOOK" in line:
                    proc.terminate()
                    break

        except KeyboardInterrupt:
            print("Monitoring stopped.")
            proc.terminate()
        print("Run Competed.")
    print("sleep 5")
    time.sleep(5)


def reset_schema(dataset: str, output_dir: Optional[str] = None):
    print(f"\nResetting schema for {dataset}")
    #nb_config = NoSQLBenchConfig(
    #    host="localhost",
    #    localdc="datacenter1",
    #    tags="block:reset-schema",
    #    alias="reset_schema",
    #    cycles=10,
    #    threads=1,
    #    errors="count,retry",
    #    workload_args={},
    #    labels=get_label_map(dataset=dataset, workload_configs={})
    #)
    #run_nosqlbench_cmd(nb_config, output_dir)

    #    DROP INDEX IF EXISTS TEMPLATE(keyspace, vector_test).TEMPLATE(table,vectors)_value_idx;
    #    DROP TABLE IF EXISTS TEMPLATE(keyspace,vector_test).TEMPLATE(table,vectors);

    create_schema(dataset, output_dir)

    cmd = []
    cmd.append('../bin/cqlsh')
    cmd.append('-e "TRUNCATE TABLE vector_test.vectors;"')
    print(" ".join(cmd))
    os.system(" ".join(cmd))


def create_schema(dataset: str, output_dir: Optional[str] = None):
    #print(f"\nSetting schema for {dataset}: (M, ef) = ({M}, {ef})...")
    #workload_args = {
    #    "dimensions": dimension_map[dataset],
    #    "M": M,
    #    "ef": ef,
    #    "similarity_function": similarity_function_map[dataset],
    #    "hdf5_path": hdf5_path_map[dataset]
    #}
    #nb_config = NoSQLBenchConfig(
    #    host="localhost",
    #    localdc="datacenter1",
    #    tags="block:schema",
    #    alias="schema",
    #    cycles=10,
    #    threads=1,
    #    errors="count,retry",
    #    workload_args=workload_args,
    #    labels=get_label_map(dataset=dataset, workload_configs=workload_args)
    #)
    #run_nosqlbench_cmd(nb_config, output_dir)

    #      CREATE KEYSPACE IF NOT EXISTS TEMPLATE(keyspace,vector_test)
    #        WITH replication = {'class': 'NetworkTopologyStrategy', 'europe-west4': '3'};
    #  - create-table: |-
    #      CREATE TABLE IF NOT EXISTS TEMPLATE(keyspace,vector_test).TEMPLATE(table,vectors) (
    #        key TEXT,
    #        value vector<float,<<dimensions:65>>>,
    #        PRIMARY KEY (key)
    #      );
    #  - create-sai-index: |-
    #      CREATE CUSTOM INDEX IF NOT EXISTS ON TEMPLATE(keyspace,vector_test).TEMPLATE(table,vectors) (value) USING 'StorageAttachedIndex'
    #      WITH OPTIONS = {'maximum_node_connections' : TEMPLATE(M,16), 'construction_beam_width' : TEMPLATE(ef,100), 'similarity_function' : 'TEMPLATE(similarity_function,dot_product)'};


    cmd = []
    cmd.append('../bin/cqlsh')
    cmd.append('-e "CREATE KEYSPACE IF NOT EXISTS vector_test WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': 1 };"')
    print(" ".join(cmd))
    os.system(" ".join(cmd))

    cmd = []
    cmd.append('../bin/cqlsh')
    cmd.append(f'-e "CREATE TABLE IF NOT EXISTS vector_test.vectors (key TEXT, value vector<float, {dimension_map[dataset]}>, PRIMARY KEY(key))"')
    print(" ".join(cmd))
    os.system(" ".join(cmd))


    cmd = []
    cmd.append('../bin/cqlsh')
    cmd.append(f'-e "CREATE CUSTOM INDEX IF NOT EXISTS ON vector_test.vectors (value) USING \'StorageAttachedIndex\'"')
    print(" ".join(cmd))
    os.system(" ".join(cmd))


def train(dataset: str, output_dir: Optional[str] = None):
    """
    NOTE: The two HNSW parameters M and ef are only used for the purpose of labeling
    """
    print(f"\nTraining for {dataset}...")
    workload_args = {
        "hdf5_path": hdf5_path_map[dataset]
    }
    nb_config = NoSQLBenchConfig(
        host="localhost",
        localdc="datacenter1",
        tags="block:rampup",
        alias=f"train",
        cycles=training_cycles_map[dataset],
        threads=2000,
        errors="count,retry,warn",
        workload_args=workload_args,
        labels=get_label_map(dataset=dataset, workload_configs=workload_args)
    )
    run_nosqlbench_cmd(nb_config, output_dir)


def test(dataset: str, output_dir: Optional[str] = None):
    print(f"\nTesting for {dataset}...")
    workload_args = {
        "hdf5_path": hdf5_path_map[dataset]
    }
    nb_config = NoSQLBenchConfig(
        host="localhost",
        localdc="datacenter1",
        tags="block:main-read",
        alias=f"test",
        cycles=test_cycles_map[dataset],
        threads=1,
        errors="count,retry,warn",
        workload_args=workload_args,
        labels=get_label_map(dataset=dataset, workload_configs=workload_args)
    )
    run_nosqlbench_cmd(nb_config, output_dir)


def list_str(val: str) -> List[str]:
    return [s.strip() for s in val.split(",")]


def list_int(val: str) -> List[int]:
    return [int(s.strip()) for s in val.split(",")]


parser = ArgumentParser()
parser.add_argument(
    "--datasets", "-d",
    required=False,
    type=list_str,
    default=list(dimension_map.keys()),
    dest="datasets",
    help="List of datasets from ann-benchmarks to run"
)
parser.add_argument(
    "--fanouts", "-M",
    required=False,
    type=list_int,
    default=[16],
    dest="fanouts",
    help="List of fanouts/M to use for HNSW index construction"
)
parser.add_argument(
    "--beam-widths", "-ef",
    required=False,
    type=list_int,
    default=[100],
    dest="beam_widths",
    help="List of beam-widths/ef to use for HNSW index construction"
)

args = parser.parse_args()
for data_set in args.datasets:
    assert data_set in dimension_map, f"Invalid ann-benchmarks dataset: {data_set}"

for data_set in args.datasets:
    output_directory = create_output_dir(data_set)
    print(f"\n=== {data_set} ===")
    reset_schema(dataset=data_set, output_dir=output_directory)
    create_schema(dataset=data_set, output_dir=output_directory)
    train(dataset=data_set, output_dir=output_directory)
    test(dataset=data_set, output_dir=output_directory)
