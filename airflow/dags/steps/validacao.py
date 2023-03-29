import os
import great_expectations as ge
import json
import pandas as pd


def get_dre_data_expectations(input_file, output_file_folder):
    """
    This function, with a income statement's input_file, creates, using great_expectations,
    some general expectations if the folder, where the expectations_file will be stored, is
    empty.
    On other hand, if the folder isn't empty, it will pass, doing nothing.

    :param input_file: Local file path of a income statemnt's data.
    :param output_file_folder: Local path of the folder, which the expectation will be stored.
    """
    # If the output_file_folder is empty, it creates a ge DataFrame.
    if not os.listdir(output_file_folder):

        df = pd.read_parquet(input_file)

        df_ge = ge.from_pandas(df)

        columns = ['Item', '2018', '2019', '2020', '2021']

        # Then, with given columns, it matches the data columns.
        df_ge.expect_table_columns_to_match_ordered_list(columns)

        # Here, it sees if the number of rows of the DataFrame is 57.
        df_ge.expect_table_row_count_to_equal(57)

        # Here, it sees that the columns' values aren't null.
        for name in df_ge.columns:

            df_ge.expect_column_values_to_not_be_null(f'{name}')

        # Finally, it saves the suite as dre_suite.json
        df_ge.save_expectation_suite(f'{output_file_folder}/dre_suite.json')

    else:

        pass


def get_bal_pat_data_expectations(input_file, output_file_folder):
    """
    This function, with a balance sheet's input_file, creates, using great_expectations,
    some general expectations if the folder, where the expectations_file will be stored, is
    empty.
    On other hand, if the folder isn't empty, it will pass, doing nothing.

    :param input_file: Local file path of a balance sheet's data.
    :param output_file_folder: Local path of the folder, which the expectation will be stored.
    """
    # If the output_file_folder is empty, it creates a ge DataFrame.
    if not os.listdir(output_file_folder):

        df = pd.read_parquet(input_file)

        df_ge = ge.from_pandas(df)

        columns = ['Item', '2018', '2019', '2020', '2021']

        # Then, with given columns, it matches the data columns.
        df_ge.expect_table_columns_to_match_ordered_list(columns)

        # Here, it sees if the number of rows of the DataFrame is 79.
        df_ge.expect_table_row_count_to_equal(79)

        # Here, it sees that the columns' values aren't null.
        for column in columns:

            df_ge.expect_column_values_to_not_be_null(f'{column}')

        # Finally, it saves the suite as bp_suite.json
        df_ge.save_expectation_suite(f'{output_file_folder}/bp_suite.json')

    else:

        pass


def get_ratio_data_expectations(input_file, output_file_folder):
    """
    This function, with a ratio's input_file, creates, using great_expectations,
    some general expectations if the folder, where the expectations_file will be stored, is
    empty.
    On other hand, if the folder isn't empty, it will pass, doing nothing.

    :param input_file: Local file path of a ratio's data.
    :param output_file_folder: Local path of the folder, which the expectation will be stored.
    """
    # If the output_file_folder is empty, it creates a ge DataFrame.
    if not os.listdir(output_file_folder):

        df = pd.read_parquet(input_file)

        df_ge = ge.from_pandas(df)

        # Here, it sees if the number of rows of the DataFrame is 4.
        df_ge.expect_table_row_count_to_equal(4)

        # Here, it sees if the values of column 'Liquidez_Corrente' are greater, or equal, than the values
        # of column 'Liquidez_Seca'. The reason for it is because 'Liquidez_Seca' is 'Liquidez_Corrente'
        # without inventory.
        df_ge.expect_column_pair_values_A_to_be_greater_than_B(
            column_A='Liquidez_Corrente',
            column_B='Liquidez_Seca',
            or_equal=True)

        # Here, it limits the values from 'Composição_do_Endividamento' to the range (-100, 100)
        df_ge.expect_column_values_to_be_between(column='Composicao_do_Endividamento', min_value=-100, max_value=100)

        columns = [
            'Liquidez_Geral', 'Liquidez_Corrente', 'Liquidez_Seca',
            'Liquidez_Imediata', 'CCL', 'Endividamento_Geral',
            'Composicao_do_Endividamento', 'Imobilizacao_do_Patrimonio_Liquido',
            'Giro_do_Ativo', 'Margem_Liquida', 'ROA', 'ROE-RSPL', 'PMRV', 'PMRE',
            'PMPC', 'CO', 'CF', 'NIG', 'ST'
        ]

        # Then, with given columns, it matches the data columns and sees if the columns' values aren't null.
        df_ge.expect_table_columns_to_match_ordered_list(columns)

        for column in columns:
            df_ge.expect_column_values_to_not_be_null(f'{column}')

        # Here, for some columns, it sees if the columns' values are greater, or equal, than 0.
        for column in [
            'PMRV', 'PMPC', 'PMRE',
            'CO', 'Liquidez_Geral',
            'Liquidez_Corrente', 'Liquidez_Seca',
            'Liquidez_Imediata'
        ]:
            df_ge.expect_column_min_to_be_between(column=column, min_value=0, max_value=None)

        # Finally, it saves the suite as ratio_suite.json
        df_ge.save_expectation_suite(f'{output_file_folder}/ratio_suite.json')

    else:

        pass


def validate_file(file_path, expectation_suite):
    """
    This function, given a data file_path and a expectation_suite, validates the data
    and returns a json dictionary of the validation's results.

    :param file_path: Local file path of the data.
    :param expectation_suite: Local file path of the expectation_suite.
    :return: The data's validation in a json format.
    """

    # Load the file into a Great Expectations Batch object.
    batch = ge.read_parquet(file_path)

    # Validate the batch using the specified expectation suite.
    result = batch.validate(expectation_suite=expectation_suite)

    # Return the validation result.
    return result.to_json_dict()


def validate_all_files(folder_path, expectation_suite, output_path):
    """
    This function, given the folder_path of the data and a expectation_suite, gets all the files
    in the folder_path and, for each file in the folder_path, it validates, using the expectation_suite,
    the data and, with the given output_path, stores, locally, the data with json format.

    :param folder_path: Local path of the data folder.
    :param expectation_suite: Local path of the expectation_suite file.
    :param output_path: Local folder, where the validation will be stored.
    """

    # Get a list of all files in the folder.
    file_list = os.listdir(folder_path)

    # Validate each file in the folder using the function validate_file.
    validation_results = {}

    for file_name in file_list:

        if file_name.endswith('.parquet'):

            file_path = os.path.join(folder_path, file_name)

            validation_result = validate_file(file_path, expectation_suite)

            validation_results[file_name] = validation_result

        # Save the validation results to a local file.
        with open(f'{output_path}/validated_{file_name.split(".")[0]}.json', 'w') as f:

            json.dump(validation_results, f)

        # Clear the validation_results to reuse it.
        validation_results = {}
