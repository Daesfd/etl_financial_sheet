import requests

import pandas as pd

from bs4 import BeautifulSoup


def get_dre_data(headers, ticker, output_file):
    url = f'https://www.marketwatch.com/investing/stock/{ticker}/financials?mod=mw_quote_tab'
    lista_de_valores, lista_de_nomes, lista_final, lista_de_exclusao = [], [], [], []
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.content, 'html.parser')

    for item_a_ser_excluido in soup.find_all('span', class_=[
        'value', 'primary', 'volume__label', 'headline', 'bg-quote', 'primary'
    ]):
        lista_de_exclusao.append(item_a_ser_excluido.string)

    for item_a_ser_excluido in soup.find_all('bg-quote', field='volume'):
        lista_de_exclusao.append(item_a_ser_excluido.string)

    for valor in soup.find_all('span'):
        lista_de_valores.append(valor.string)

    lista_de_valores = [elemento for elemento in lista_de_valores if elemento not in (
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

    lista_de_valores = [elemento for elemento in lista_de_valores if elemento not in lista_de_exclusao]
    new_val = lista_de_valores[43:328]

    for nome in soup.select('div.cell__content.fixed--cell'):
        lista_de_nomes.append(nome.string)

    lista_de_nomes = [e for e in lista_de_nomes if e not in ('5-year trend', None)]

    lista_final.append(lista_de_nomes.pop(0))
    for ano in ['2018', '2019', '2020', '2021']:
        lista_final.append(ano)

    while True:
        try:
            lista_final.append(lista_de_nomes.pop(0))
            new_val.pop(0)

            for i in range(0, 4):
                lista_final.append(new_val.pop(0))

        except IndexError:
            break

    data = list(zip(*[iter(lista_final)] * 5))

    df = pd.DataFrame(data)

    df_header = df.iloc[0]

    df.columns = df_header

    df = df[1:]

    df.to_csv(output_file)


def get_bal_pat(headers, ticker, output_file):
    url = f'https://www.marketwatch.com/investing/stock/{ticker}/financials/balance-sheet'
    lista_de_valores, lista_de_nomes, lista_final, lista_de_exclusao = [], [], [], []
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.content, 'html.parser')

    for item_a_ser_excluido in soup.find_all('span', class_=[
        'value', 'primary', 'volume__label', 'headline', 'bg-quote', 'primary'
    ]):
        lista_de_exclusao.append(item_a_ser_excluido.string)

    for item_a_ser_excluido in soup.find_all('bg-quote', field='volume'):
        lista_de_exclusao.append(item_a_ser_excluido.string)

    for valor in soup.find_all('span'):
        lista_de_valores.append(valor.string)

    lista_de_valores = [elemento for elemento in lista_de_valores if elemento not in (
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

    lista_de_valores = [elemento for elemento in lista_de_valores if elemento not in lista_de_exclusao]

    new_val = lista_de_valores[43:439]
    new_val.pop(180)

    for nome in soup.select('div.cell__content.fixed--cell'):
        lista_de_nomes.append(nome.string)

    lista_de_nomes = [e for e in lista_de_nomes if e not in ('5-year trend', None)]
    lista_de_nomes.pop(37)

    lista_final.append(lista_de_nomes.pop(0))
    for ano in ['2018', '2019', '2020', '2021']:
        lista_final.append(ano)

    while True:
        try:
            lista_final.append(lista_de_nomes.pop(0))
            new_val.pop(0)

            for i in range(0, 4):
                lista_final.append(new_val.pop(0))

        except IndexError:
            break

    data = list(zip(*[iter(lista_final)] * 5))

    df = pd.DataFrame(data)

    df_header = df.iloc[0]

    df.columns = df_header

    df = df[1:]

    df.to_csv(output_file)
