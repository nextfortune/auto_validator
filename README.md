Great-Expectations Usage
===
## Table of Contents
- [Great-Expectations Usage](#Great-Expectations-Usage)
  - [Table of Contents](#Table-of-Contents)
  - [CLI Commands](#CLI-Commands)
  - [Yaml Descriptions](#Yaml-Descriptions)
  - [Example YAML](#Example-YAML)

## CLI Commands
- Init GE projects
    ```
    great_expectations --v3-api init
    ```
- List Datasource of GE
    ```
    great_expectations --v3-api datasource list
    ```
    remove **--v3-api** to see more details of datasource
- Delete Datasource of GE
    ```
    great_expectations --v3-api datasource delete <YOUR_DATA_SOURCE_NAME>
    ```
- List Suite of GE
    ```
    great_expectations --v3-api suite list
    ```
    remove **--v3-api** to see more details of datasource
- Delete Suite of GE
    ```
    great_expectations --v3-api suite delete <YOUR_DATA_SOURCE_NAME>
    ```
- Run GE with Yaml
    ```
    python3 execute_ge.py -f <YOUR_YAML_FILE>
    ```
    `<YOUR_YAML_FILE>` example: /home/user/project/example.yml

## Yaml Descriptions
- usage:
    * init: Start everything new
    * edit_suite: Only edit existing suite for new rule
    * check: Only run checkpoints 
- datasource_name: `<YOUR_DATASOURCE_NAME>`
- datasource_path: `<YOUR_DATASOURCE_PATH>`
- example_file_name: `<YOUR_EXAMPLE_FILE_NAME>`
- file_with_header: 
    * True:  file with header
    * False: file without header
- expectation_suite_name: `<YOUR_SUITE_NAME>`
- testing_file_pattern: `<YOUR_TESTING_FILE_PATTERN>`
- rules:
    * ignored_columns: A Columns List not to check in suite
    * excluded_expectations: A Rule List not to include in suite
    * primary_key: A Columns List that should be unique in value
- edit_columns:
    * table: add table rules
        - Rule name 
            - Rule setting
    * column_name: add column rules
        - Rule name
            - Rule setting

## Example YAML

* File without Header
    ```yaml
    great_expectation:
      usage: init
      datasource_name: example_use
      datasource_path: /home/sid/projects/ge_test/data
      example_file_name: blue_tripdata_sample_2019-01.csv
      file_with_header: False
      expectation_suite_name: example_suite_without_header
      testing_file_pattern: blue_tripdata_sample_*.csv
      #expectation rules
      rules:
        #skip checking columns 6 and 10
        ignored_columns: [
          # _c0, 
          # _c1, 
          # _c2, 
          # _c3, 
          # _c4, 
          # _c5, 
          _c6, 
          # _c7, 
          # _c8, 
          # _c9, 
          _c10, 
          # _c11, 
          # _c12, 
          # _c13, 
          # _c14, 
          # _c15, 
          # _c16, 
          # _c17
        ]
        #skip checking mean and median
        excluded_expectations: [
          # expect_table_columns_to_match_ordered_list,
          # expect_table_row_count_to_be_between,
          # expect_column_min_to_be_between,
          # expect_column_max_to_be_between,
          expect_column_mean_to_be_between,
          expect_column_median_to_be_between,
          # expect_column_quantile_values_to_be_between,
          # expect_column_values_to_be_in_set,
          # expect_column_values_to_not_be_null,
          # expect_column_values_to_be_in_type_list,
          # expect_column_values_to_not_be_null,
          # expect_column_proportion_of_unique_values_to_be_between,
        ]
        #no primary key
        primary_key: []

      #advanced columns setting
      edit_columns:
        _c1: #add rules to column 1
          expect_column_values_to_match_strftime_format: #check datetime str format
            strftime_format: "%Y-%m-%d %H:%M:%S"
        _c12: #add rules to column 12
          expect_column_stdev_to_be_between: #check standard deviation
            min_value: -5
            max_value: 5
      #more rules details at https://legacy.docs.greatexpectations.io/en/stable/autoapi/great_expectations/expectations/index.html


* File with Header
    ```yaml
    great_expectation:
      usage: edit_suite
      datasource_name: example_use
      datasource_path: /home/sid/projects/ge_test/data
      example_file_name: yellow_tripdata_sample_2019-01.csv
      file_with_header: True
      expectation_suite_name: example_suite_with_header
      testing_file_pattern: yellow_tripdata_sample_*.csv
      #expectation rules
      rules:
        #skip checking tolls_amount, improvement_surcharge, total_amount, congestion_surcharge
        ignored_columns: [
          # vendor_id, 
          # pickup_datetime, 
          # dropoff_datetime, 
          # passenger_count, 
          # trip_distance, 
          # rate_code_id, 
          # store_and_fwd_flag, 
          # pickup_location_id, 
          # dropoff_location_id, 
          # payment_type, 
          # fare_amount, 
          # extra, 
          # mta_tax, 
          # tip_amount, 
          tolls_amount, 
          improvement_surcharge, 
          total_amount, 
          congestion_surcharge
        ]
        #skip checking rule column_proportion_of_unique_values
        excluded_expectations: [
          # expect_table_columns_to_match_ordered_list,
          # expect_table_row_count_to_be_between,
          # expect_column_min_to_be_between,
          # expect_column_max_to_be_between,
          # expect_column_mean_to_be_between,
          # expect_column_median_to_be_between,
          # expect_column_quantile_values_to_be_between,
          # expect_column_values_to_be_in_set,
          # expect_column_values_to_not_be_null,
          # expect_column_values_to_be_in_type_list,
          # expect_column_values_to_not_be_null,
          expect_column_proportion_of_unique_values_to_be_between
        ]
        #vendor_id as  primary key
        primary_key: [
          vendor_id,
        ]

      #advanced columns setting
      edit_columns:
        vendor_id: #add rules to vendor_id column
          expect_column_values_to_be_between:  #check values in range(1,10)
            min_value: 1
            max_value: 5
          expect_column_most_common_value_to_be_in_set: #check most common value in [0,2,5]
            value_set: [2,3]
        passenger_count:  #add rules to passenger_count column
          expect_column_values_to_be_between: #check values in range(1,20)
            min_value: 0
            max_value: 20
          expect_column_most_common_value_to_be_in_set: #check most common value in [1,2,3]
            value_set: [1,3]
      #more rules details at https://legacy.docs.greatexpectations.io/en/stable/autoapi/great_expectations/expectations/index.html