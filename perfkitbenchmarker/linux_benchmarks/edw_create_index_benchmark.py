# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""

Run command:

BigQuery:

./pkb.py \
--cloud=GCP  \
--benchmarks=edw_create_index_benchmark \
--bq_client_interface=PYTHON  \
--config_override=edw_create_index_benchmark.edw_service.type=bigquery \
--config_override=edw_create_index_benchmark.edw_service.cluster_identifier=p3rf-bq-search.search_index_dataset \
--gcp_service_account=bigquery-testing-pkb@p3rf-bigquery-smallquery-slots.iam.gserviceaccount.com \
--gcp_service_account_key_file=/home/shuninglin/p3rf-bq-search-050c6559ed66.json \
--edw_index_creation_query_dir=edw/bigquery/search_index/CUJ1 \
--edw_power_queries=verify_no_index_query,create_index_query,delete_index_query,check_index_coverage_query \
--metadata=cloud:GCP \
--project=p3rf-bq-search \
--zones=us-central1-c 

Snowflake:

TODO: Add SF queries to query folder and set up SF tables.

./pkb.py \
--cloud=AWS \
--benchmarks=edw_create_index_benchmark \
--snowflake_client_interface=JDBC  \
--config_override=edw_create_index_benchmark.edw_service.type=snowflake_aws \
--config_override=edw_index_benchmark.edw_service.cluster_identifier= \
--snowflake_database=SEARCH_INDEX \
--snowflake_schema=INDEX_TEST \
--snowflake_warehouse=XSMALL_TEST \
--edw_index_creation_query_dir=edw/snowflake_aws/search_index/CUJ1 \
--edw_power_queries=verify_no_index_query,create_index_query,delete_index_query,check_index_coverage_query \
--metadata=cloud:AWS \
--snowflake_jdbc_client_jar=/home/shuninglin/snowflake-jdbc-client-2.13-enterprise.jar \
--machine_type=m4.large \
--zones=us-west-2a
"""

"""Benchmark for creating an index in an EDW service."""

import os
import time
import logging
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

BENCHMARK_NAME = 'edw_create_index_benchmark'

BENCHMARK_CONFIG = """
edw_create_index_benchmark:
  description: Benchmark for creating an index in an EDW service.
  edw_service:
    type: bigquery
    cluster_identifier: _cluster_id_
  vm_groups:
    client:
      vm_spec: *default_dual_core
"""

flags.DEFINE_string(
    'edw_index_creation_query_dir',
    '',
    'Optional local directory containing all query files. '
    'Can be absolute or relative to the executable.',
)

FLAGS = flags.FLAGS


def GetConfig(user_config):
  """Loads and returns the benchmark config."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Prepares the client VM to run the benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  benchmark_spec.always_call_cleanup = True
  edw_service_instance = benchmark_spec.edw_service
  vm = benchmark_spec.vms[0]

  edw_service_instance.GetClientInterface().SetProvisionedAttributes(
      benchmark_spec
  )
  edw_service_instance.GetClientInterface().Prepare('edw_common')

  query_locations = [
      os.path.join(FLAGS.edw_index_creation_query_dir, query)
      for query in FLAGS.edw_power_queries.split(',')
  ]
  any(vm.PushDataFile(query_loc) for query_loc in query_locations)


# Check if there's an index already created. If so, issue a command to delete the index and keep
# checking until index is deleted or timeout
def ensure_no_index(client_interface):
  start_time = time.time()
  timeout = 30
  while True:
    time_elapsed = time.time() - start_time
    if time_elapsed > timeout:
      logging.error("Timed out waiting for index to be deleted.")
      # TODO: find a way to stop the benchmark in case of timeout
      break
    _, metadata = client_interface.ExecuteQuery('verify_no_index_query')
    if metadata and metadata.get('rows_returned', 0) > 0:
      client_interface.ExecuteQuery('delete_index_query')
    else:
      break
    time.sleep(1) 


# Create the index
def create_index(client_interface, results):
  execution_time, metadata = client_interface.ExecuteQuery('create_index_query')
  results.append(sample.Sample('search_index_creation_time', execution_time, 'seconds', metadata))


# Check Index Coverage until it reaches 100, record the time of reaching 100.
# Time out if it takes too long to reach 100
def measure_building_time(client_interface, results):
  start_time = time.time()
  timeout = 120
  while True:
    _, metadata = client_interface.ExecuteQuery('check_index_coverage_query')
    time_elapsed = time.time() - start_time
    if metadata and metadata.get('rows_returned', 0) > 0:
      results.append(sample.Sample('search_index_available_time', time_elapsed, 'seconds', metadata))
      break
    if time_elapsed > timeout:
      logging.error("Timed out waiting for index to fully cover the table.")
      # TODO: find a way to stop the benchmark in case of timeout
      break
    else:
      time.sleep(1) 


def Run(benchmark_spec):
  """Runs the benchmark and returns a list of samples.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
  results = []

  edw_service_instance = benchmark_spec.edw_service
  client_interface = edw_service_instance.GetClientInterface()

  ensure_no_index(client_interface)

  create_index(client_interface, results)

  measure_building_time(client_interface, results)

  return results


def Cleanup(benchmark_spec):
  """Cleans up the benchmark resources.

  Args:
    benchmark_spec: The benchmark specification.
  """
  benchmark_spec.edw_service.Cleanup()
  edw_service_instance = benchmark_spec.edw_service
  client_interface = edw_service_instance.GetClientInterface()
  client_interface.ExecuteQuery('delete_index_query')  
