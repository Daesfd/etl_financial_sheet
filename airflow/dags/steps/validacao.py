import os
import great_expectations as ge
import json
import pandas as pd


def get_dre_data_expectations(input_file, output_file_folder):

    if not os.listdir(output_file_folder):

        df = pd.read_parquet(input_file)

        df_ge = ge.from_pandas(df)

        df_ge.expect_table_columns_to_match_ordered_list(df_ge.columns)

        df_ge.expect_table_row_count_to_equal(57)

        for name in df_ge.columns:

            df_ge.expect_column_values_to_not_be_null(f'{name}')

            df_ge.expect_column_to_exist(f'{name}')

        df_ge.save_expectation_suite(f'{output_file_folder}/dre_suite.json')

    else:

        return None


def get_bal_pat_data_expectations(input_file, output_file_folder):

    if not os.listdir(output_file_folder):

        df = pd.read_parquet(input_file)

        df_ge = ge.from_pandas(df)

        columns = ['Item', '2018', '2019', '2020', '2021']

        df_ge.expect_table_columns_to_match_ordered_list(columns)

        df_ge.expect_table_row_count_to_equal(4)

        for name in columns:
            df_ge.expect_column_values_to_not_be_null(f'{name}')

        df_ge.save_expectation_suite(f'{output_file_folder}/bp_suite.json')

    else:

        return None


def get_ratio_data_expectations(input_file, output_file_folder):

    if not os.listdir(output_file_folder):

        df = pd.read_parquet(input_file)

        df_ge = ge.from_pandas(df)

        df_ge.expect_table_row_count_to_equal(4)

        df_ge.expect_column_pair_values_A_to_be_greater_than_B(
            column_A='Liquidez_Corrente',
            column_B='Liquidez_Seca',
            or_equal=True)

        df_ge.expect_column_values_to_be_between(column='Composicao_do_Endividamento', min_value=-100, max_value=100)

        columns = [
            'Liquidez_Geral', 'Liquidez_Corrente', 'Liquidez_Seca',
            'Liquidez_Imediata', 'CCL', 'Endividamento_Geral',
            'Composicao_do_Endividamento', 'Imobilizacao_do_Patrimonio_Liquido',
            'Giro_do_Ativo', 'Margem_Liquida', 'ROA', 'ROE-RSPL', 'PMRV', 'PMRE',
            'PMPC', 'CO', 'CF', 'NIG', 'ST'
        ]

        df_ge.expect_table_columns_to_match_ordered_list(columns)

        for column in columns:
            df_ge.expect_column_values_to_not_be_null(f'{column}')

        for column in [
            'PMRV', 'PMPC', 'PMRE',
            'CO', 'Liquidez_Geral',
            'Liquidez_Corrente', 'Liquidez_Seca',
            'Liquidez_Imediata'
        ]:
            df_ge.expect_column_min_to_be_between(column=column, min_value=0, max_value=None)

        df_ge.save_expectation_suite(f'{output_file_folder}/ratio_suite.json')

    else:

        return None


def validate_file(file_path, expectation_suite):
    # Load the file into a Great Expectations Batch object
    batch = ge.read_parquet(file_path)

    # Validate the batch using the specified expectation suite
    result = batch.validate(expectation_suite=expectation_suite)

    # Return the validation result
    return result.to_json_dict()


def validate_all_files(folder_path, expectation_suite, output_path):
    # Get a list of all files in the folder
    file_list = os.listdir(folder_path)

    # Validate each file in the folder
    validation_results = {}
    for file_name in file_list:
        if file_name.endswith('.parquet'):
            file_path = os.path.join(folder_path, file_name)
            validation_result = validate_file(file_path, expectation_suite)
            validation_results[file_name] = validation_result

        # Save the validation results to a file
        with open(f'{output_path}/validated_{file_name.split(".")[0]}.json', 'w') as f:
            json.dump(validation_results, f)

        validation_results = {}
