import requests

import pandas as pd

from bs4 import BeautifulSoup


def get_dre_data(headers, ticker, output_file):
    url = f'https://www.marketwatch.com/investing/stock/{ticker}/financials?mod=mw_quote_tab'
    value_list, name_list, final_list, exclusion_list = [], [], [], []
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.content, 'html.parser')

    for excluded_item in soup.find_all('span', class_=[
        'value', 'primary', 'volume__label', 'headline', 'bg-quote', 'primary'
    ]):
        exclusion_list.append(excluded_item.string)

    for excluded_item in soup.find_all('bg-quote', field='volume'):
        exclusion_list.append(excluded_item.string)

    for value in soup.find_all('span'):
        value_list.append(value.string)

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

    value_list = [value for value in value_list if value not in exclusion_list]
    value_list = value_list[43:328]

    for name in soup.select('div.cell__content.fixed--cell'):
        name_list.append(name.string)

    name_list = [e for e in name_list if e not in ('5-year trend', None)]

    final_list.append(name_list.pop(0))
    for year in ['2018', '2019', '2020', '2021']:
        final_list.append(year)

    while True:
        try:
            final_list.append(name_list.pop(0))
            value_list.pop(0)

            for i in range(0, 4):
                final_list.append(value_list.pop(0))

        except IndexError:
            break

    data = list(zip(*[iter(final_list)] * 5))

    df = pd.DataFrame(data)

    df_header = df.iloc[0]

    df.columns = df_header

    df = df[1:]

    df.to_csv(output_file, index=False)


def get_bal_pat_data(headers, ticker, output_file):
    url = f'https://www.marketwatch.com/investing/stock/{ticker}/financials/balance-sheet'
    value_list, name_list, final_list, exclusion_list = [], [], [], []
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.content, 'html.parser')

    for excluded_item in soup.find_all('span', class_=[
        'value', 'primary', 'volume__label', 'headline', 'bg-quote', 'primary'
    ]):
        exclusion_list.append(excluded_item.string)

    for excluded_item in soup.find_all('bg-quote', field='volume'):
        exclusion_list.append(excluded_item.string)

    for value in soup.find_all('span'):
        value_list.append(value.string)

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

    value_list = [value for value in value_list if value not in exclusion_list]

    value_list = value_list[43:439]
    value_list.pop(180)

    for name in soup.select('div.cell__content.fixed--cell'):
        name_list.append(name.string)

    name_list = [name for name in name_list if name not in ('5-year trend', None)]
    name_list.pop(37)

    final_list.append(name_list.pop(0))
    for year in ['2018', '2019', '2020', '2021']:
        final_list.append(year)

    while True:
        try:
            final_list.append(name_list.pop(0))
            value_list.pop(0)

            for i in range(0, 4):
                final_list.append(value_list.pop(0))

        except IndexError:
            break

    data = list(zip(*[iter(final_list)] * 5))

    df = pd.DataFrame(data)

    df_header = df.iloc[0]

    df.columns = df_header

    df = df[1:]

    df.to_csv(output_file, index=False)
