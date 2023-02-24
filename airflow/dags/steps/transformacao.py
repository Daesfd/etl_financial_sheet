import pandas as pd


def data_cleaning(input_file, output_file):

    df = pd.read_csv(input_file)

    for year in ['2018', '2019', '2020', '2021']:

        df[year] = df[year].str.replace('(', '-', regex=True).str.replace(')', '', regex=True)

        for item in range(0, len(df)):

            df[year][item] = df[year][item].replace(',', '')

            if df[year][item] == '-':

                df[year][item] = df[year][item].replace('-', '')

                if len(df[year][item]) == 0:

                    df[year][item] = 0

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

            else:

                df[year][item] = float(df[year][item])

    df['2018'][4] = 0

    df.astype({'Item':str, '2018': float, '2019': float, '2020': float, '2021': float}).to_parquet(output_file, index=False)


def get_ratios(balanco_patrimonial_data_file, dre_data_file, output_file):
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

    liq_ger, liq_cor, liq_seca, liq_im, ccl_data = [], [], [], [], []
    end_ger, comp_end, imob_pl = [], [], []
    giro_at, marg_liq, roa, roe = [], [], [], []
    pmrv_lista, pmre_lista, pmpc_lista = [], [], []
    co, cf = [], []
    nig, st = [], []

    df_bal = pd.read_parquet(balanco_patrimonial_data_file)
    df_dre = pd.read_parquet(dre_data_file)

    for year in ['2018', '2019', '2020', '2021']:

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

    pmpc_lista.pop(0)
    pmpc_lista.insert(0, (pmpc_lista[0] + pmpc_lista[1] + pmpc_lista[2]) / 3)

    for i in range(len(pmre_lista)):
        co.append(calc_co(pmre=pmre_lista[i], pmrv=pmrv_lista[i]))
        cf.append(calc_cf(co=co[i], pmpc=pmpc_lista[i]))

    df_com_indicadores = pd.DataFrame({
        'Liquidez Geral': liq_ger,
        'Liquidez Corrente': liq_cor,
        'Liquidez Seca': liq_cor,
        'Liquidez Imediata': liq_im,
        'CCL': ccl_data,
        'Endividamento Geral': end_ger,
        'Composicao do Endividamento': comp_end,
        'Imobilizacao do Patrimonio Liquido': imob_pl,
        'Giro do Ativo': giro_at,
        'Margem Liquida': marg_liq,
        'ROA': roa,
        'ROE/RSPL': roe,
        'PMRV': pmrv_lista,
        'PMRE': pmre_lista,
        'PMPC': pmpc_lista,
        'CO': co,
        'CF': cf,
        'NIG': nig,
        'ST': st
    }, index=['2018', '2019', '2020', '2021'])

    df_com_indicadores.to_parquet(output_file)


def avg_sector_values(
        path_to_lren_ind,
        path_to_hnory_ind,
        path_to_frg_ind,
        path_to_nxgpy_ind,
        output_path_file
):
    l, col = [], []
    years = ['2018', '2019', '2020', '2021']
    lren_ind = pd.read_parquet(path_to_lren_ind)
    nxgpy_ind = pd.read_parquet(path_to_nxgpy_ind)
    hnory_ind = pd.read_parquet(path_to_hnory_ind)
    frg_ind = pd.read_parquet(path_to_frg_ind)

    for ratio in lren_ind.columns:

        for year in years:

            col.append(lren_ind.loc[year, ratio])
            col.append(nxgpy_ind.loc[year, ratio])
            col.append(hnory_ind.loc[year, ratio])
            col.append(frg_ind.loc[year, ratio])

            media = sum(col[:4]) / 4

            col.clear()

            l.append(media)

    data = list(zip(*[iter(l)] * 4))

    df_media = pd.DataFrame(data).T
    df_media.set_index(pd.Index(years), inplace=True)
    df_media.columns = lren_ind.columns

    df_media.to_parquet(output_path_file, index=True)
