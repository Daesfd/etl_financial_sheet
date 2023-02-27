import requests

import pandas as pd

from bs4 import BeautifulSoup


def get_dre_data(headers, ticker, output_file):
    """
    This function, with a given stock name, creates a dataframe containing the
    uncleaned data of the stock's income statement.

    :param headers: Headers used for the requests' library.
    :param ticker: Name of the stock.
    :param output_file: Path of the dataframe that will be created.
    """
    url = f'https://www.marketwatch.com/investing/stock/{ticker}/financials?mod=mw_quote_tab'
    value_list, name_list, final_list, exclusion_list = [], [], [], []
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.content, 'html.parser')

    # There are some strings, which creates distinct mutable patterns for the collected data.
    # So, in those line below, it saves the string which will be excluded.
    for excluded_item in soup.find_all('span', class_=[
        'value', 'primary', 'volume__label', 'headline', 'bg-quote', 'primary'
    ]):
        exclusion_list.append(excluded_item.string)

    for excluded_item in soup.find_all('bg-quote', field='volume'):
        exclusion_list.append(excluded_item.string)

    # Here, it puts all the wanted data, which contains the values of the items, into a list.
    for value in soup.find_all('span'):
        value_list.append(value.string)

    # Because of others distinct patterns, there are another process to clean the raw data.
    value_list = [value for value in value_list if value not in (
        ' Content',
        'Assets',
        'LRENY Finanicals',
        'FRG Finanicals',
        'MGLUY Finanicals',
        'Advertisement',
        'Add Tickers',
        'Recently Viewed Tickers',
        'United States',
        None)]

    # Here, it excludes the data, based on the excluded list.
    # Moreover, it restricts the data, which contains only the values.
    value_list = [value for value in value_list if value not in exclusion_list]
    value_list = value_list[43:328]

    # As the same with the values, the name data is collected, and it excludes some data.
    for name in soup.select('div.cell__content.fixed--cell'):
        name_list.append(name.string)

    name_list = [name for name in name_list if name not in ('5-year trend', None)]

    # Finally, it appends the first item in the name list, while removing, of the original list, the item.
    final_list.append(name_list.pop(0))

    # The date that will be used will be inserted into the final_list.
    for year in ['2018', '2019', '2020', '2021']:
        final_list.append(year)

    # While there are name in the name_list and values in the value_list, the loop
    # add them into the final_list.
    while True:
        try:
            # Firstly, it appends the first name, of the name list, to the final_list.
            # And then it pops the first value of the value_list, which won't be used,
            # because of the year is not compatible.
            final_list.append(name_list.pop(0))
            value_list.pop(0)

            # Then, it appends 4 values ti the final list, in a way that the final list will
            # be composed as:
            # Item 2018  2019  2020  2021 name value value value value name value value value value ...
            for i in range(0, 4):

                final_list.append(value_list.pop(0))

        except IndexError:
            break

    # Lastly, it creates a list containing lists the dataframe rows, separating the step above as:
    # Item 2018  2019  2020  2021
    # name value value value value
    # name value value value value
    # And creates a dataframe with the columns as the first row and save it locally.
    data = list(zip(*[iter(final_list)] * 5))

    df = pd.DataFrame(data)

    df_header = df.iloc[0]

    df.columns = df_header

    df = df[1:]

    df.to_csv(output_file, index=False)


def get_bal_pat_data(headers, ticker, output_file):
    """
    This function, with a given stock name, creates a dataframe containing the
    uncleaned data of the stock's financial sheet.

    :param headers: Headers used for the requests' library.
    :param ticker: Name of the stock.
    :param output_file: Path of the dataframe that will be created.
    """
    url = f'https://www.marketwatch.com/investing/stock/{ticker}/financials/balance-sheet'
    value_list, name_list, final_list, exclusion_list = [], [], [], []
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.content, 'html.parser')

    # There are some strings, which creates distinct mutable patterns for the collected data.
    # So, in those line below, it saves the string which will be excluded.
    for excluded_item in soup.find_all('span', class_=[
        'value', 'primary', 'volume__label', 'headline', 'bg-quote', 'primary'
    ]):
        exclusion_list.append(excluded_item.string)

    for excluded_item in soup.find_all('bg-quote', field='volume'):
        exclusion_list.append(excluded_item.string)

    # Here, it puts all the wanted data, which contains the values of the items, into a list.
    for value in soup.find_all('span'):
        value_list.append(value.string)

    # Because of others distinct patterns, there are another process to clean the raw data.
    value_list = [value for value in value_list if value not in (
        ' Content',
        'Assets',
        'LRENY Finanicals',
        'FRG Finanicals',
        'MGLUY Finanicals',
        'Advertisement',
        'Add Tickers',
        'Recently Viewed Tickers',
        'United States',
        None)]

    # Here, it excludes the data, based on the excluded list.
    # Moreover, it restricts the data, which contains only the values and pops an undesirable value.
    value_list = [value for value in value_list if value not in exclusion_list]

    value_list = value_list[43:439]
    value_list.pop(180)

    # As the same with the values, the name data is collected, and it excludes some data.
    for name in soup.select('div.cell__content.fixed--cell'):
        name_list.append(name.string)

    name_list = [name for name in name_list if name not in ('5-year trend', None)]
    name_list.pop(37)

    # Finally, it appends the first item in the name list, while removing, of the original list, the item.
    final_list.append(name_list.pop(0))

    # The date that will be used will be inserted into the final_list.
    for year in ['2018', '2019', '2020', '2021']:
        final_list.append(year)

    # While there are name in the name_list and values in the value_list, the loop
    # add them into the final_list.
    while True:
        try:
            # Firstly, it appends the first name, of the name list, to the final_list.
            # And then it pops the first value of the value_list, which won't be used,
            # because of the year is not compatible.
            final_list.append(name_list.pop(0))
            value_list.pop(0)

            for i in range(0, 4):
                # Then, it appends 4 values ti the final list, in a way that the final list will
                # be composed as:
                # Item 2018  2019  2020  2021 name value value value value name value value value value ...
                final_list.append(value_list.pop(0))

        except IndexError:
            break

    # Lastly, it creates a list containing lists the dataframe rows, separating the step above as:
    # Item 2018  2019  2020  2021
    # name value value value value
    # name value value value value
    # And creates a dataframe with the columns as the first row and save it locally.
    data = list(zip(*[iter(final_list)] * 5))

    df = pd.DataFrame(data)

    df_header = df.iloc[0]

    df.columns = df_header

    df = df[1:]

    df.to_csv(output_file, index=False)
