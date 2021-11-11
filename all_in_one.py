import os
import fnmatch
from ruamel import yaml

import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import BatchRequest
from great_expectations.profile.user_configurable_profiler import (
    UserConfigurableProfiler,
)
from great_expectations.validator.validator import Validator

context = ge.get_context()
# NOTE: The following assertion is only for testing and can be ignored by users.
assert context

# First configure a new Datasource and add to DataContext
datasource_name = "all_in_one_test"
datasource_path = "/home/sid/projects/ge_test/data/"

datasource_yaml = f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SparkDFExecutionEngine
data_connectors:
  default_inferred_data_connector_name:
    class_name: InferredAssetFilesystemDataConnector
    base_directory: {datasource_path}
    default_regex:
      group_names:
        - data_asset_name
      pattern: (.*)
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
"""

context.test_yaml_config(datasource_yaml)
context.add_datasource(**yaml.load(datasource_yaml))

#sanitize_yaml_and_save_datasource(context, example_yaml, overwrite_existing=False)

# Get Validator by creating ExpectationSuite and passing in BatchRequest
example_file_name = "yellow_tripdata_sample_2019-01.csv"
expectation_suite_name = "all_in_one_suite"

batch_request = BatchRequest(
    datasource_name=datasource_name,
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name=example_file_name ,
    limit=1000,
    batch_spec_passthrough={"reader_method": "csv", "reader_options": {"header":True, "inferSchema":True}},
)
#above batch_spec_passthrough optional for pyspark reader setting
context.create_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)
# NOTE: The following assertion is only for testing and can be ignored by users.
assert isinstance(validator, Validator)

# Profile the data with the UserConfigurableProfiler and save resulting ExpectationSuite
ignored_columns = [
    # "vendor_id",
    # "pickup_datetime",
    # "dropoff_datetime",
    # # "passenger_count",
    # "trip_distance",
    # "rate_code_id",
    # "store_and_fwd_flag",
    # "pickup_location_id",
    # "dropoff_location_id",
    # "payment_type",
    # "fare_amount",
    # "extra",
    # "mta_tax",
    # "tip_amount",
    # "tolls_amount",
    # "improvement_surcharge",
    # "total_amount",
    # "congestion_surcharge",
]

profiler = UserConfigurableProfiler(
    profile_dataset=validator,
    excluded_expectations=None,
    ignored_columns=ignored_columns,
    not_null_only=False,
    primary_or_compound_key=False,
    semantic_types_dict=None,
    table_expectations_only=False,
    value_set_threshold="MANY",
)
suite = profiler.build_suite()
validator.save_expectation_suite(discard_failed_expectations=False)

# Create first checkpoint on example file
my_checkpoint_config = f"""
name: checkpoint on example file
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-check-suite"
validations:
  - batch_request:
      datasource_name: {datasource_name }
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: {example_file_name }
      data_connector_query:
        index: -1
      batch_spec_passthrough: 
        reader_method: csv
        reader_options: 
          header: True
          inferSchema: True
    expectation_suite_name: {expectation_suite_name }
"""
#above batch_spec_passthrough optional for header and schema
my_checkpoint_config = yaml.load(my_checkpoint_config)

# NOTE: The following code (up to and including the assert) is only for testing and can be ignored by users.
# In the current test, site_names are set to None because we do not want to update and build data_docs
# If you would like to build data_docs then either remove `site_names=None` or pass in a list of site_names you would like to build the docs on.
checkpoint = SimpleCheckpoint(
    **my_checkpoint_config, data_context=context,
)
checkpoint_result = checkpoint.run()
assert checkpoint_result.run_results

#get all filename in path
def files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file

pattern = "yellow_tripdata_sample_*.csv"
test_files=[]
for file in files(datasource_path):
    #if filename match pattern add to test_files
    if fnmatch.fnmatch(file, pattern):
        test_files.append(file)

for test_file in test_files:
  # Create second checkpoint on testing file
  testing_file_name = test_file
  my_new_checkpoint_config = f"""
  name: checkpoint on testing file
  config_version: 1.0
  class_name: SimpleCheckpoint
  run_name_template: "%Y%m%d-%H%M%S-check-{testing_file_name }"
  validations:
    - batch_request:
        datasource_name: {datasource_name }
        data_connector_name: default_inferred_data_connector_name
        data_asset_name: {testing_file_name }
        data_connector_query:
          index: -1
        batch_spec_passthrough: 
          reader_method: csv
          reader_options: 
            header: True
            inferSchema: True
      expectation_suite_name: {expectation_suite_name }
  """
  #above batch_spec_passthrough optional for header and schema
  my_new_checkpoint_config = yaml.load(my_new_checkpoint_config)

  # NOTE: The following code (up to and including the assert) is only for testing and can be ignored by users.
  # In the current test, site_names are set to None because we do not want to update and build data_docs
  # If you would like to build data_docs then either remove `site_names=None` or pass in a list of site_names you would like to build the docs on.
  new_checkpoint = SimpleCheckpoint(
      **my_new_checkpoint_config, data_context=context,
  )
  new_checkpoint_result = new_checkpoint.run()
  assert new_checkpoint_result.run_results

#save result to localsite html
context.build_data_docs()