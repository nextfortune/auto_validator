import os
import json
import fnmatch
import argparse
from ruamel import yaml
from dataclasses import dataclass

import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import BatchRequest
from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
from great_expectations.validator.validator import Validator
from great_expectations.exceptions.exceptions import DataContextError

context = ge.get_context()
# NOTE: The following assertion is only for testing and can be ignored by users.
assert context

parser = argparse.ArgumentParser(description='YAML file')
parser.add_argument('-f', dest='yml_file', type=str, help='Name of yaml file')
args = parser.parse_args()

class MyYaml():
    def __init__(self, x):
        with open(args.yml_file, 'r') as stream:
            parsed_yaml=yaml.safe_load(stream)
        
        for key, value in parsed_yaml[x].items():
            setattr(self, key, value)

@dataclass
class GreatExpectations:
    usage: str=None
    datasource_name: str=None
    datasource_path: str=None
    example_file_name: str=None
    file_with_header: str=None
    expectation_suite_name: str=None
    testing_file_pattern: str=None
    #expectation rules
    rules: str=None
    #advanced editing on expectations
    edit_columns: str=None

@dataclass
class Rules:
    ignored_columns: list=None
    excluded_expectations: list=None


def list_unvalidated_files(configuration_class):
    unvalidated_files=[]
    for file in os.listdir(configuration_class.datasource_path):
        if os.path.isfile(os.path.join(configuration_class.datasource_path, file)):
            if fnmatch.fnmatch(file, configuration_class.testing_file_pattern):
                unvalidated_files.append(file)
    return unvalidated_files


def execute_datasource(configuration_class):
    # First configure a new Datasource and add to DataContext
    datasource_yaml = f"""
name: {configuration_class.datasource_name}
class_name: Datasource
execution_engine:
  class_name: SparkDFExecutionEngine
data_connectors:
  default_inferred_data_connector_name:
    class_name: InferredAssetFilesystemDataConnector
    base_directory: {configuration_class.datasource_path}
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
        datasource_name=configuration_class.datasource_name,
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name=configuration_class.example_file_name,
        limit=1000,
        batch_spec_passthrough={"reader_method": "csv", "reader_options": {"header": configuration_class.file_with_header, "inferSchema":True}},
    )
    return batch_request


def execute_suite(configuration_class, batch_request: BatchRequest=None):
    if configuration_class.usage == "init":
        try:
            context.create_expectation_suite(expectation_suite_name=configuration_class.expectation_suite_name)
        except DataContextError:
            pass

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=configuration_class.expectation_suite_name,
    )
    # NOTE: The following assertion is only for testing and can be ignored by users.
    assert isinstance(validator, Validator)

    rules = Rules(**configuration_class.rules)

    # Profile the data with the UserConfigurableProfiler and save resulting ExpectationSuite
    profiler = UserConfigurableProfiler(
        profile_dataset=validator,
        excluded_expectations=rules.excluded_expectations,
        ignored_columns=rules.ignored_columns,
        not_null_only=False,
        primary_or_compound_key=None,
        semantic_types_dict=None,
        table_expectations_only=False,
        value_set_threshold="MANY",
    )
    suite = profiler.build_suite()

    #check advanced tags
    if hasattr(configuration_class, 'edit_columns'):
        #set advanced tags as customerize setting
        for key, value in configuration_class.edit_columns.items():
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
      datasource_name: {configuration_class.datasource_name }
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: {configuration_class.example_file_name }
      data_connector_query:
        index: -1
      batch_spec_passthrough: 
        reader_method: csv
        reader_options: 
          header: {configuration_class.file_with_header}
          inferSchema: True
    expectation_suite_name: {configuration_class.expectation_suite_name }
    """

    my_checkpoint_config = yaml.load(my_checkpoint_config)

    checkpoint = SimpleCheckpoint(
        **my_checkpoint_config, data_context=context,
    )
    checkpoint_result = checkpoint.run()
    assert checkpoint_result.run_results


def execute_checkpoint(configuration_class, file: str):
    # Create second checkpoint on testing file
    my_new_checkpoint_config = f"""
  name: checkpoint on testing file
  config_version: 1.0
  class_name: SimpleCheckpoint
  run_name_template: "%Y%m%d-%H%M%S-check-{ file }"
  validations:
    - batch_request:
        datasource_name: { configuration_class.datasource_name }
        data_connector_name: default_inferred_data_connector_name
        data_asset_name: { file }
        data_connector_query:
          index: -1
        batch_spec_passthrough: 
          reader_method: csv
          reader_options: 
            header: { configuration_class.file_with_header }
            inferSchema: True
      expectation_suite_name: { configuration_class.expectation_suite_name }
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
    assert new_checkpoint_result.run_results
    return {
        "pattern": configuration_class.testing_file_pattern,
        "name": file,
        "status": str(new_checkpoint_result["success"]),
        "failed_rules": failed_rules,
        "statistics": statistic,
    }


def main():
    #Read Yaml Setting
    with open(args.yml_file, 'r') as stream:
        try:
            parsed_yaml=yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    great_expectation = GreatExpectations(**parsed_yaml["great_expectation"])
    # for key, value in parsed_yaml.items():
    #     exec(key + '=MyYaml(key)')

    batch_request = execute_datasource(great_expectation)

    execute_suite(great_expectation, batch_request)

    files = list_unvalidated_files(great_expectation)

    validation_results = []
    for file in files:
        validation_results.append(execute_checkpoint(great_expectation, file))

    #save result to localsite html
    context.build_data_docs()

    json_string = json.dumps(validation_results)
    print(json_string)


if __name__ == "__main__":
    main()