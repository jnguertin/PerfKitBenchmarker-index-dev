# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
# limitations under the License
r"""Runs a BigQuery index search benchmark.

Sample command:
blaze run third_party/py/perfkitbenchmarker:pkb -- \
  --benchmarks=bigquery_index_search \
  --gce_network_name=default \
  --gce_subnet_name=default \
  --gcloud_scopes=https://www.googleapis.com/auth/bigquery
"""
import json
import logging
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import sample
from perfkitbenchmarker.providers.gcp import util as gcp_util

BENCHMARK_NAME = 'bigquery_index_search'
BENCHMARK_CONFIG = """
bigquery_index_search:
  description: Runs a python script to benchmark BigQuery index search.
  flags:
    cloud: GCP
    gcloud_scopes: >
      https://www.googleapis.com/auth/bigquery
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n1-standard-16
          zone: us-central1-f
"""

REMOTE_SCRIPT = 'perfkitbenchmarker/data/edw/bigquery/bigquery_index_search.py'
UNIT = 'seconds'
FLAGS = flags.FLAGS


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install and set up the benchmark on the target VM."""
  vm = benchmark_spec.vms[0]
  vm.Install('google_cloud_sdk')
  vm.RemoteCommand(f'gcloud auth application-default login')
  vm.PushFile(REMOTE_SCRIPT)
  vm.RemoteCommand(f'sudo chmod 755 {REMOTE_SCRIPT}')


def Run(benchmark_spec):
  """Run a benchmark python script on a VM and returns results."""
  vm = benchmark_spec.vms[0]
  project_id = FLAGS.project or gcp_util.GetDefaultProject()
  cmd = (
      f'python3 {REMOTE_SCRIPT} --project_id={project_id} '
      f'--dataset_id={benchmark_spec.uuid}'
  )
  logging.info(cmd)
  stdout, stderr = vm.RemoteCommand(cmd)
  logging.info(stdout)
  logging.info(stderr)
  results = json.loads(stdout)
  return [
      sample.Sample(
          'BigQuery index search', results['execution_time'], UNIT, {}
      )
  ]


def Cleanup(benchmark_spec):
  """Cleanup the benchmark."""
  vm = benchmark_spec.vms[0]
  project_id = FLAGS.project or gcp_util.GetDefaultProject()
  dataset_id = benchmark_spec.uuid
  vm.RemoteCommand(
      f'bq rm --project_id={project_id} -r -f -d {dataset_id}',
      ignore_failure=True,
  )
