import pandas as pd
import matplotlib.pyplot as plt


def data_cleaning(input_file, output_file):
    """
    Function, which receives an uncleaned CSV dataframe, with improper values, like 10B (10 Billions),
    and returns a cleaned PARQUET dataframe, with proper values.

    :param input_file: Path file of the uncleaned dataframe.
    :param output_file: Path file of the dataframe that will be cleaned.
    """

    df = pd.read_csv(input_file)

    for year in ['2018', '2019', '2020', '2021']:

        # As the negatives numbers are written as, for exemple, (10M), there is the necessity
        # to transform the value to -10M
        df[year] = df[year].str.replace('(', '-', regex=True).str.replace(')', '', regex=True)

        for item in range(0, len(df)):

            # There are some values written ,as, for exemple, 3,100.0, so the code below
            # transform the value into a better way (3100.0)
            df[year][item] = df[year][item].replace(',', '')

            # There are some blank values, so, in the code below, the value is transformed into 0
            if df[year][item] == '-':

                df[year][item] = df[year][item].replace('-', '')

                if len(df[year][item]) == 0:

                    df[year][item] = 0

            # The non-blanked values are ended with either:
            # 1. B for billion
            # 2. M for million
            # 3. K for thousand
            # 4. % for percentage
            # So, the code below transform each value into a float, being million based.
            elif df[year][item].endswith('B'):

                df[year][item] = float(df[year][item].split('B')[0]) * 1000

            elif df[year][item].endswith('M'):

                df[year][item] = float(df[year][item].split('M')[0])

            elif df[year][item].endswith('K'):

                df[year][item] = float(df[year][item].split('K')[0]) / 1000

            elif df[year][item].endswith('%'):

                df[year][item] = float(df[year][item].split('%')[0]) / 100

            elif len(df[year][item]) == 0:

                df[year][item] = 0

            # Furthermore, if none of the situations don't occour, then the value is the float of itself.
            else:

                df[year][item] = float(df[year][item])

    # Because there are some unknown errors in the extration part, it is needed to assign
    # the already blanked value as df['2018][4] as 0,
    df['2018'][4] = 0

    # Finally, the cleaned dataframe is saved locally as a Parquet file.
    df.astype({'Item': str, '2018': float, '2019': float, '2020': float, '2021': float}).to_parquet(output_file, index=False)


def get_ratios(balanco_patrimonial_data_file, dre_data_file, output_file):
    """
    This function receives the cleaned PARQUET files of the income statement and financial sheet,
    creates, bases on those two files, a dataframe of financial ratios and saves locally it as
    a PARQUET file.

    :param balanco_patrimonial_data_file: Path file of the financial sheet data.
    :param dre_data_file: Path file of the income sheet data.
    :param output_file: Path file of the financial ratio dataframe that will be created.

    :param df_bal: Another name for the financial sheet data.
    :param df_dre: Another name for the income statement data.
    :oaram year: Year of the column of the financial sheet or income statement dataframe.
    """

    # Below there are the calculations of the ratios.
    def calc_liq_geral(df_bal, year):
        return (df_bal[year][19] + df_bal[year][29]) / df_bal[year][59]

    def calc_liq_corrente(df_bal, year):
        return df_bal[year][19] / df_bal[year][46]

    def calc_liq_seca(df_bal, year):
        return (df_bal[year][19] - df_bal[year][12]) / df_bal[year][46]

    def calc_liq_imediata(df_bal, year):
        return df_bal[year][0] / df_bal[year][46]

    def calc_ccl(df_bal, year):
        return df_bal[year][19] - df_bal[year][46]

    def calc_end_geral(df_bal, year):
        return df_bal[year][59] / df_bal[year][34] * 100

    def calc_comp_end(df_bal, year):
        return df_bal[year][46] / df_bal[year][59] * 100

    def calc_imob_pl(df_bal, year):
        return (df_bal[year][20] + df_bal[year][27] + df_bal[year][30]) / df_bal[year][77] * 100

    def calc_giro_at(df_dre, df_bal, year):
        return df_dre[year][0] / df_bal[year][34]

    def calc_marg_liq(df_dre, year):
        return df_dre[year][38] / df_dre[year][0]

    def calc_roa(df_dre, df_bal, year):
        return df_dre[year][38] / df_bal[year][34]

    def calc_roe(df_dre, df_bal, year):
        return df_dre[year][38] / df_bal[year][77]

    def calc_pmrv(df_bal, df_dre, year):
        return df_bal[year][5] / df_dre[year][0] * 360

    def calc_pmre(df_bal, df_dre, year):
        return df_bal[year][12] / abs(df_dre[year][2]) * 360

    def calc_pmpc(df_bal, df_dre, year):
        if year == '2018':
            return 0
        return df_bal[year][39] / (abs(df_dre[year][2]) + df_bal[year][12] - df_bal[str(int(year) - 1)][12]) * 360

    def calc_co(pmre, pmrv):
        return pmre + pmrv

    def calc_cf(co, pmpc):
        return co - pmpc

    def calc_nig(df_bal, year):
        aco = df_bal[year][5] + df_bal[year][12]
        pco = df_bal[year][39]
        return aco - pco

    def calc_st(df_bal, year):
        acf = df_bal[year][0]
        pcf = df_bal[year][36] + df_bal[year][41]
        return acf - pcf

    # To create a dataframe, we will use lists, which contains the sequential data for
    # each of the columns, being, at index 0, the data of year 2019 and so on.
    liq_ger, liq_cor, liq_seca, liq_im, ccl_data = [], [], [], [], []
    end_ger, comp_end, imob_pl = [], [], []
    giro_at, marg_liq, roa, roe = [], [], [], []
    pmrv_lista, pmre_lista, pmpc_lista = [], [], []
    co, cf = [], []
    nig, st = [], []

    df_bal = pd.read_parquet(balanco_patrimonial_data_file)
    df_dre = pd.read_parquet(dre_data_file)

    for year in ['2018', '2019', '2020', '2021']:

        # Foe each function, it will append the data in the list.
        liq_ger.append(calc_liq_geral(df_bal=df_bal, year=year))
        liq_cor.append(calc_liq_corrente(df_bal=df_bal, year=year))
        liq_seca.append(calc_liq_seca(df_bal=df_bal, year=year))
        liq_im.append(calc_liq_imediata(df_bal=df_bal, year=year))

        ccl_data.append(calc_ccl(df_bal=df_bal, year=year))
        end_ger.append(calc_end_geral(df_bal=df_bal, year=year))
        comp_end.append(calc_comp_end(df_bal=df_bal, year=year))
        imob_pl.append(calc_imob_pl(df_bal=df_bal, year=year))

        giro_at.append(calc_giro_at(df_dre=df_dre, df_bal=df_bal, year=year))
        marg_liq.append(calc_marg_liq(df_dre=df_dre, year=year))
        roa.append(calc_roa(df_dre=df_dre, df_bal=df_bal, year=year))
        roe.append(calc_roe(df_dre=df_dre, df_bal=df_bal, year=year))

        pmrv_lista.append(calc_pmrv(df_bal=df_bal, df_dre=df_dre, year=year))
        pmre_lista.append(calc_pmre(df_bal=df_bal, df_dre=df_dre, year=year))
        pmpc_lista.append(calc_pmpc(df_bal=df_bal, df_dre=df_dre, year=year))

        nig.append(calc_nig(df_bal=df_bal, year=year))
        st.append(calc_st(df_bal=df_bal, year=year))

    # Because pmpc needs data from 2017, it pops the first value (0) and insert
    # the average of the years 2019, 2020 and 2021 at 2018 data.
    pmpc_lista.pop(0)
    pmpc_lista.insert(0, (pmpc_lista[0] + pmpc_lista[1] + pmpc_lista[2]) / 3)

    for i in range(len(pmre_lista)):
        co.append(calc_co(pmre=pmre_lista[i], pmrv=pmrv_lista[i]))
        cf.append(calc_cf(co=co[i], pmpc=pmpc_lista[i]))

    # Creation of the dataframe with columns as the lists created.
    df_com_indicadores = pd.DataFrame({
        'Liquidez_Geral': liq_ger,
        'Liquidez_Corrente': liq_cor,
        'Liquidez_Seca': liq_cor,
        'Liquidez_Imediata': liq_im,
        'CCL': ccl_data,
        'Endividamento_Geral': end_ger,
        'Composicao_do_Endividamento': comp_end,
        'Imobilizacao_do_Patrimonio_Liquido': imob_pl,
        'Giro_do_Ativo': giro_at,
        'Margem_Liquida': marg_liq,
        'ROA': roa,
        'ROE-RSPL': roe,
        'PMRV': pmrv_lista,
        'PMRE': pmre_lista,
        'PMPC': pmpc_lista,
        'CO': co,
        'CF': cf,
        'NIG': nig,
        'ST': st
    }, index=['2018', '2019', '2020', '2021'])

    # FInally, the ratio dataframe is saved locally as a Parquet file.
    df_com_indicadores.to_parquet(output_file)


def avg_sector_values(
        path_to_lren_ind,
        path_to_hnory_ind,
        path_to_frg_ind,
        path_to_nxgpy_ind,
        output_path_file
):
    """
    This function receives 4 files, with each of them containg the financial ratios data of the stock.
    Then, it creates a dataframe with each ratio's average.
    :param path_to_lren_ind: LREN stock's ratio data's file path.
    :param path_to_hnory_ind: HNORY stock's ratio data's file path.
    :param path_to_frg_ind: FRG stock's ratio data's file path.
    :param path_to_nxgpy_ind: NXGPY stock's ratio data's file path.
    :param output_path_file: File path of the average ratio dataframe.
    """

    l, col = [], []
    years = ['2018', '2019', '2020', '2021']
    lren_ind = pd.read_parquet(path_to_lren_ind)
    nxgpy_ind = pd.read_parquet(path_to_nxgpy_ind)
    hnory_ind = pd.read_parquet(path_to_hnory_ind)
    frg_ind = pd.read_parquet(path_to_frg_ind)

    for ratio in lren_ind.columns:

        for year in years:

            # In those lines, each years' ratios' data of each dataframe is inserted into a list.
            col.append(lren_ind.loc[year, ratio])
            col.append(nxgpy_ind.loc[year, ratio])
            col.append(hnory_ind.loc[year, ratio])
            col.append(frg_ind.loc[year, ratio])

            # Then, it takes the average of the ratio, cleans the list to take the others ratios
            # and append the average value in a new list.
            media = sum(col[:4]) / 4

            col.clear()

            l.append(media)

    # With the average data from each year in the list, it creates a list made of lists, in a way
    # that the inner list is composed of the yearly data of the ratio.
    # For exemple:
    # ((ratio_1_2018, ratio_1_2019, ratio_1_2020, ratio_1_2021),
    # (ratio_2_2018, ratio_2_2019, ratio_2_2020, ratio_2_2021), ...).
    data = list(zip(*[iter(l)] * 4))

    # With the data, it creates a dataframe, transposing the data, and sets the index and the columns.
    df_media = pd.DataFrame(data).T
    df_media.set_index(pd.Index(years), inplace=True)
    df_media.columns = lren_ind.columns

    # Finally, the dataframe is saved locally as a Parquet file.
    df_media.to_parquet(output_path_file, index=True)


def get_images(avg_path_file, mglu_ratio_path_file, image_file_path):
    """
    This function receives the average ratio data and the chosen stock ratio data and creates
    comparison images for each ratio.

    :param avg_path_file: File path of the average ratio data.
    :param mglu_ratio_path_file: File path of the chosen stock ratio data.
    :param image_file_path: File path of the images taht will be created.
    """

    df_setor = pd.read_parquet(avg_path_file)
    df_mglu = pd.read_parquet(mglu_ratio_path_file)

    for Item in df_setor.columns:

        # For each ratio, it is plotted the ratio of the sector and the chosen stock.
        # Finally, it saves the plot.
        plt.plot(df_setor.index, df_setor[Item], label=Item + ' Setor')
        plt.plot(df_mglu.index, df_mglu[Item], label=Item + ' MGLU')

        plt.xlabel('Ano')
        plt.ylabel('Valor')
        plt.legend()

        plt.savefig(f'{image_file_path}/{Item}.png')
        plt.clf()
