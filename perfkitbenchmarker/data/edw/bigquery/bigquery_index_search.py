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
"""Runs a BigQuery index search benchmark."""
import json
import logging
import time

from absl import app
from absl import flags
from google.cloud import bigquery

FLAGS = flags.FLAGS

flags.DEFINE_string('project_id', None, 'GCP project to use for the benchmark.')
flags.DEFINE_string('dataset_id', None, 'BigQuery dataset to use.')
flags.DEFINE_string('table_id', 'unindexed_table', 'Table to load data into.')
flags.DEFINE_string(
    'indexed_table_id', 'indexed_table', 'Table to create with an index.'
)
flags.DEFINE_string(
    'query',
    'SELECT * FROM `{project}.{dataset}.{table}` WHERE SEARCH(review, "good")',
    'Query to run.',
)
flags.DEFINE_string(
    'gcs_uri',
    'gs://pkb-yellore-data/yellore-json-data/review.json',
    'GCS URI of the data to load.',
)


def main(argv):
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')

  client = bigquery.Client(project=FLAGS.project_id)

  # Create unindexed table and load data.
  unindexed_table_ref = client.dataset(FLAGS.dataset_id).table(FLAGS.table_id)
  job_config = bigquery.LoadJobConfig(
      source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
      autodetect=True,
  )
  load_job = client.load_table_from_uri(
      FLAGS.gcs_uri, unindexed_table_ref, job_config=job_config
  )
  load_job.result()

  # Create indexed table from unindexed table.
  indexed_table_ref = client.dataset(FLAGS.dataset_id).table(
      FLAGS.indexed_table_id
  )
  create_indexed_table_sql = f"""
  CREATE TABLE `{indexed_table_ref.project}.{indexed_table_ref.dataset_id}.{indexed_table_ref.table_id}`
  AS SELECT * FROM `{unindexed_table_ref.project}.{unindexed_table_ref.dataset_id}.{unindexed_table_ref.table_id}`
  """
  client.query(create_indexed_table_sql).result()

  # Add index to the indexed table.
  alter_table_sql = f"""
  ALTER TABLE `{indexed_table_ref.project}.{indexed_table_ref.dataset_id}.{indexed_table_ref.table_id}`
  ADD SEARCH INDEX `review_index` (ALL COLUMNS)
  """
  client.query(alter_table_sql).result()

  # Wait for index to be created.
  while True:
    table = client.get_table(indexed_table_ref)
    if table.indexes:
      break
    time.sleep(10)

  # Run query and measure execution time.
  query = FLAGS.query.format(
      project=FLAGS.project_id,
      dataset=FLAGS.dataset_id,
      table=FLAGS.indexed_table_id,
  )
  start_time = time.time()
  client.query(query).result()
  end_time = time.time()
  execution_time = end_time - start_time

  # Output results as JSON.
  results = {
      'execution_time': execution_time,
  }
  print(json.dumps(results))


if __name__ == '__main__':
  app.run(main)
