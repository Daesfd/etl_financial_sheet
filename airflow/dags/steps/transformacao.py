import pandas as pd


def limpeza_dos_dados(input_file, output_file):

    df = pd.read_csv(input_file)

    for ano in ['2018', '2019', '2020', '2021']:

        df[ano] = df[ano].str.replace('(', '-', regex=True).str.replace(')', '', regex=True)

        for item in range(0, len(df)):

            #df[ano][item] = df[ano][item].replace(',', '')

            if df[ano][item] == '-':

                df[ano][item] = df[ano][item].replace('-', '')

                if len(df[ano][item]) == 0:

                    df[ano][item] = 0

            elif df[ano][item].endswith('B'):

                df[ano][item] = float(df[ano][item].split('B')[0]) * 1000

            elif df[ano][item].endswith('M'):

                df[ano][item] = float(df[ano][item].split('M')[0])

            elif df[ano][item].endswith('K'):

                df[ano][item] = float(df[ano][item].split('K')[0]) / 1000

            elif df[ano][item].endswith('%'):

                df[ano][item] = float(df[ano][item].split('%')[0]) / 100

            elif len(df[ano][item]) == 0:

                df[ano][item] = 0

            else:

                df[ano][item] = float(df[ano][item])

    df['2018'][4] = 0

    df.astype({'Item':str, '2018': float, '2019': float, '2020': float, '2021': float}).to_parquet(output_file, index=False)


def get_indicadores(balanco_patrimonial_data_file, dre_data_file, output_file):
    def calc_liq_geral(df_bal, year):
        return (df_bal[year][20] + df_bal[year][30]) / df_bal[year][60]

    def calc_liq_corrente(df_bal, year):
        return df_bal[year][20] / df_bal[year][47]

    def calc_liq_seca(df_bal, year):
        return (df_bal[year][20] - df_bal[year][13]) / df_bal[year][47]

    def calc_liq_imediata(df_bal, year):
        return df_bal[year][1] / df_bal[year][47]

    def calc_ccl(df_bal, year):
        return df_bal[year][20] - df_bal[year][47]

    def calc_end_geral(df_bal, year):
        return df_bal[year][60] / df_bal[year][35] * 100

    def calc_comp_end(df_bal, year):
        return df_bal[year][47] / df_bal[year][60] * 100

    def calc_imob_pl(df_bal, year):
        return (df_bal[year][21] + df_bal[year][28] + df_bal[year][31]) / df_bal[year][78] * 100

    def calc_giro_at(df_dre, df_bal, year):
        return df_dre[year][1] / df_bal[year][35]

    def calc_marg_liq(df_dre, year):
        return df_dre[year][39] / df_dre[year][1]

    def calc_roa(df_dre, df_bal, year):
        return df_dre[year][39] / df_bal[year][35]

    def calc_roe(df_dre, df_bal, year):
        return df_dre[year][39] / df_bal[year][78]

    def calc_pmrv(df_bal, df_dre, year):
        return df_bal[year][6] / df_dre[year][1] * 360

    def calc_pmre(df_bal, df_dre, year):
        return df_bal[year][13] / abs(df_dre[year][3]) * 360

    def calc_pmpc(df_bal, df_dre, year):
        if year == '2018':
            return 0
        return df_bal[year][40] / (abs(df_dre[year][3]) + df_bal[year][13] - df_bal[str(int(year) - 1)][13]) * 360

    def calc_co(pmre, pmrv):
        return pmre + pmrv

    def calc_cf(co, pmpc):
        return co - pmpc

    def calc_nig(df_bal, year):
        aco = df_bal[year][6] + df_bal[year][13]
        pco = df_bal[year][40]
        return aco - pco

    def calc_st(df_bal, year):
        acf = df_bal[year][1]
        pcf = df_bal[year][37] + df_bal[year][42]
        return acf - pcf

    liq_ger, liq_cor, liq_seca, liq_im, ccl_data = [], [], [], [], []
    end_ger, comp_end, imob_pl = [], [], []
    giro_at, marg_liq, roa, roe = [], [], [], []
    pmrv_lista, pmre_lista, pmpc_lista = [], [], []
    co, cf = [], []
    nig, st = [], []

    df_bal = pd.read_parquet(balanco_patrimonial_data_file)
    df_dre = pd.read_parquet(dre_data_file)

    for ano in ['2018', '2019', '2020', '2021']:

        liq_ger.append(calc_liq_geral(df_bal=df_bal, year=ano))
        liq_cor.append(calc_liq_corrente(df_bal=df_bal, year=ano))
        liq_seca.append(calc_liq_seca(df_bal=df_bal, year=ano))
        liq_im.append(calc_liq_imediata(df_bal=df_bal, year=ano))

        ccl_data.append(calc_ccl(df_bal=df_bal, year=ano))
        end_ger.append(calc_end_geral(df_bal=df_bal, year=ano))
        comp_end.append(calc_comp_end(df_bal=df_bal, year=ano))
        imob_pl.append(calc_imob_pl(df_bal=df_bal, year=ano))

        giro_at.append(calc_giro_at(df_dre=df_dre, df_bal=df_bal, year=ano))
        marg_liq.append(calc_marg_liq(df_dre=df_dre, year=ano))
        roa.append(calc_roa(df_dre=df_dre, df_bal=df_bal, year=ano))
        roe.append(calc_roe(df_dre=df_dre, df_bal=df_bal, year=ano))

        pmrv_lista.append(calc_pmrv(df_bal=df_bal, df_dre=df_dre, year=ano))
        pmre_lista.append(calc_pmre(df_bal=df_bal, df_dre=df_dre, year=ano))
        pmpc_lista.append(calc_pmpc(df_bal=df_bal, df_dre=df_dre, year=ano))

        nig.append(calc_nig(df_bal=df_bal, year=ano))
        st.append(calc_st(df_bal=df_bal, year=ano))

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


def obtencao_da_media_do_setor(
        path_to_lren_ind,
        path_to_hnory_ind,
        path_to_frg_ind,
        path_to_nxgpy_ind,
        output_path_file
):
    anos = ['2018', '2019', '2020', '2021']
    l, col = [], []

    lren_ind = pd.read_parquet(path_to_lren_ind)
    nxgpy_ind = pd.read_parquet(path_to_nxgpy_ind)
    hnory_ind = pd.read_parquet(path_to_hnory_ind)
    frg_ind = pd.read_parquet(path_to_frg_ind)

    for indice in lren_ind.columns:

        for ano in anos:

            col.append(lren_ind.loc[ano, indice])
            col.append(nxgpy_ind.loc[ano, indice])
            col.append(hnory_ind.loc[ano, indice])
            col.append(frg_ind.loc[ano, indice])

            media = sum(col[:4]) / 4

            col.clear()

            l.append(media)

    data = list(zip(*[iter(l)] * 4))

    df_media = pd.DataFrame(data).T
    df_media.set_index(pd.Index(anos), inplace=True)
    df_media.columns = lren_ind.columns

    df_media.to_parquet(output_path_file, index=False)
