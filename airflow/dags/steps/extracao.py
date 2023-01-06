import requests

import pandas as pd

from bs4 import BeautifulSoup


def get_dre_data(headers, ticker, output_file):
    url = f'https://www.marketwatch.com/investing/stock/{ticker}/financials?mod=mw_quote_tab'
    val, ls, list3 = [], [], []
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.content, 'html.parser')

    valores = soup.select('span')

    for valor in valores:
        val.append(valor.string)

    val = [e for e in val if
           e not in (' Content', 'Assets', 'LREN3 Finanicals', '002251 Finanicals', 'MGLUY Finanicals')]

    new_val = val[93:378]

    for l in soup.select('div.cell__content.fixed--cell'):
        ls.append(l.string)

    ls = [e for e in ls if e not in '5-year trend']

    new_ls = list(filter(None, ls))

    list3.append(new_ls.pop(0))
    for ano in range(2017, 2022):
        list3.append(ano)

    while True:
        try:
            list3.append(new_ls.pop(0))

            for i in range(0, 5):
                list3.append(new_val.pop(0))

        except IndexError:
            break

    data = list(zip(*[iter(list3)] * 6))

    df = pd.DataFrame(data)

    df_header = df.iloc[0]

    df.columns = df_header

    df = df[1:]

    df.to_csv(output_file)


def get_bal_pat(header, ticker, output_file):
    url = f'https://www.marketwatch.com/investing/stock/{ticker}/financials/balance-sheet'
    val, ls, list3 = [], [], []
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.content, 'html.parser')

    valores = soup.select('span')

    for valor in valores:
        val.append(valor.string)

    val = [e for e in val if
           e not in (' Content', 'Assets', 'LREN3 Finanicals', '002251 Finanicals', 'MGLUY Finanicals')]

    val.pop(272)
    new_val = val[92:487]

    for l in soup.select('div.cell__content.fixed--cell'):
        ls.append(l.string)

    ls = [e for e in ls if e not in '5-year trend']

    ls.pop(37)

    new_ls = list(filter(None, ls))

    list3.append(new_ls.pop(0))
    for ano in range(2017, 2022):
        list3.append(ano)

    while True:
        try:
            list3.append(new_ls.pop(0))

            for i in range(0, 5):
                list3.append(new_val.pop(0))

        except IndexError:
            break

    data = list(zip(*[iter(list3)] * 6))

    df = pd.DataFrame(data)

    df.to_csv(output_file)
