import os
import json
import fnmatch
import argparse
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

parser = argparse.ArgumentParser(description='YAML file')
parser.add_argument('-f', dest='yml_file', type=str, help='Name of yaml file')
args = parser.parse_args()
#Read Yaml Setting
with open(args.yml_file, 'r') as stream:
    try:
        parsed_yaml=yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

class MyYaml():
    def __init__(self, x):
        with open(args.yml_file, 'r') as stream:
            parsed_yaml=yaml.safe_load(stream)
        
        for key, value in parsed_yaml[x].items():
            setattr(self, key, value)

for key, value in parsed_yaml.items():
    exec(key + '=MyYaml(key)')

for key, value in great_expectation.rules.items():
    # print(key)
    exec(key + '=value')
# First configure a new Datasource and add to DataContext
if great_expectation.usage != 'check':
    datasource_yaml = f"""
name: {great_expectation.datasource_name}
class_name: Datasource
execution_engine:
  class_name: SparkDFExecutionEngine
data_connectors:
  default_inferred_data_connector_name:
    class_name: InferredAssetFilesystemDataConnector
    base_directory: {great_expectation.datasource_path}
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

    # Get Validator by creating ExpectationSuite and passing in BatchRequest
    batch_request = BatchRequest(
        datasource_name=great_expectation.datasource_name,
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name=great_expectation.example_file_name,
        limit=1000,
        batch_spec_passthrough={"reader_method": "csv", "reader_options": {"header": great_expectation.file_with_header, "inferSchema":True}},
    )

    if great_expectation.usage == "init":
        context.create_expectation_suite(expectation_suite_name=great_expectation.expectation_suite_name)
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=great_expectation.expectation_suite_name,
    )
    # NOTE: The following assertion is only for testing and can be ignored by users.
    assert isinstance(validator, Validator)
    
    # Profile the data with the UserConfigurableProfiler and save resulting ExpectationSuite
    profiler = UserConfigurableProfiler(
        profile_dataset=validator,
        excluded_expectations=excluded_expectations,
        ignored_columns=ignored_columns,
        not_null_only=False,
        primary_or_compound_key=None,
        semantic_types_dict=None,
        table_expectations_only=False,
        value_set_threshold="MANY",
    )
    suite = profiler.build_suite()

    #check advanced tags
    if hasattr(great_expectation, 'edit_columns'):
        #set advanced tags as customerize setting
        for key, value in great_expectation.edit_columns.items():
            for k,v in value.items():
                if key == 'table':
                    getattr(validator, k)(**v)
                else:
                    getattr(validator, k)(column=key, **v)

    validator.save_expectation_suite(discard_failed_expectations=False)

    # Create first checkpoint on example file
    my_checkpoint_config = f"""
name: checkpoint on example file
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-check-suite"
validations:
  - batch_request:
      datasource_name: {great_expectation.datasource_name }
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: {great_expectation.example_file_name }
      data_connector_query:
        index: -1
      batch_spec_passthrough: 
        reader_method: csv
        reader_options: 
          header: {great_expectation.file_with_header}
          inferSchema: True
    expectation_suite_name: {great_expectation.expectation_suite_name }
    """

    my_checkpoint_config = yaml.load(my_checkpoint_config)

    checkpoint = SimpleCheckpoint(
        **my_checkpoint_config, data_context=context,
    )
    checkpoint_result = checkpoint.run()
    assert checkpoint_result.run_results

def files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file

list_file=[]
for file in files(great_expectation.datasource_path):
    if fnmatch.fnmatch(file, great_expectation.testing_file_pattern):
        list_file.append(file)
result_list=[]
for test_file in list_file:
  # Create second checkpoint on testing file
  testing_file_name = test_file
  my_new_checkpoint_config = f"""
  name: checkpoint on testing file
  config_version: 1.0
  class_name: SimpleCheckpoint
  run_name_template: "%Y%m%d-%H%M%S-check-{testing_file_name }"
  validations:
    - batch_request:
        datasource_name: {great_expectation.datasource_name }
        data_connector_name: default_inferred_data_connector_name
        data_asset_name: {testing_file_name }
        data_connector_query:
          index: -1
        batch_spec_passthrough: 
          reader_method: csv
          reader_options: 
            header: {great_expectation.file_with_header}
            inferSchema: True
      expectation_suite_name: {great_expectation.expectation_suite_name }
  """
  my_new_checkpoint_config = yaml.load(my_new_checkpoint_config)

  new_checkpoint = SimpleCheckpoint(
      **my_new_checkpoint_config, data_context=context,
  )
  new_checkpoint_result = new_checkpoint.run()
  failed_rules=[]
  for key, value in new_checkpoint_result['run_results'].items():
      for k, v in value.items():
          if k == 'validation_result':
              statistic = v['statistics']
            
              for i in range(len(v['results'])):
                #   print(i,": ", v['results'][i])
                  if v['results'][i]['success'] is False:
                      failed = {v['results'][i]['expectation_config']['kwargs']['column'] : v['results'][i]['expectation_config']['expectation_type']}
                      failed_rules.append(failed)
  validate_dict={
    "pattern": great_expectation.testing_file_pattern,
    "name": testing_file_name,
    "status": str(new_checkpoint_result["success"]),
    "failed_rules": failed_rules,
    "statistics": statistic,
  }
  result_list.append(validate_dict)
  assert new_checkpoint_result.run_results

json_string = json.dumps(result_list)
print(json_string)
    # sys.exit("Validation with Great Expectations failed.")
    # raise AirflowException("Validation with Great Expectations failed.")
#save result to localsite html
context.build_data_docs()