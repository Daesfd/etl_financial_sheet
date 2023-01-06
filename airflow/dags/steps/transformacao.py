import pandas as pd


def limpeza_dos_dados(input_file, output_file):

    df = pd.read_parquet(input_file)

    for ano in range(2017, 2022):

        df[ano] = df[ano].str.replace('(', '-', regex=True).str.replace(')', '', regex=True)

        for linha in range(1, len(df) + 1):

            if df[ano][linha] == '-':

                df = df.replace('-', '')

            elif df[ano][linha].endswith('B'):

                df[ano][linha] = float(df[ano][linha].split('B')[0]) * 1000

            elif df[ano][linha].endswith('M'):

                df[ano][linha] = float(df[ano][linha].split('M')[0])

    df[2017][2] = None

    df.to_csv(output_file)


def get_indicadores(balanco_patrimonial_data_file, dre_data_file, output_file):
    df_bal = pd.read_parquet(balanco_patrimonial_data_file)
    df_dre = pd.read_parquet(dre_data_file)

    liq_ger, liq_cor, liq_seca, liq_im, ccl_data = [], [], [], [], []
    end_ger, comp_end, imob_pl = [], [], []
    giro_at, marg_liq, roa, roe = [], [], [], []

    for i in range(2017, 2022):
        liquidez_geral = (df_bal[i][20] + df_bal[i][30]) / df_bal[i][60]
        liq_ger.append(liquidez_geral)

        liquidez_corrente = df_bal[i][20] / df_bal[i][47]
        liq_cor.append(liquidez_corrente)

        liquidez_seca = (df_bal[i][20] - df_bal[i][13]) / df_bal[i][47]
        liq_seca.append(liquidez_seca)

        liquidez_imediata = (df_bal[i][1] / df_bal[i][47])
        liq_im.append(liquidez_imediata)

        ccl = (df_bal[i][20] - df_bal[i][47])
        ccl_data.append(ccl)

        endividamento_geral = (df_bal[i][60] / df_bal[i][35] * 100)
        end_ger.append(endividamento_geral)

        composicao_do_endividamento = (df_bal[i][47] / df_bal[i][60] * 100)
        comp_end.append(composicao_do_endividamento)

        imobilizacao_do_patrimonio_liquido = (df_bal[i][21] + df_bal[i][28] + df_bal[i][31]) / df_bal[i][78] * 100
        imob_pl.append(imobilizacao_do_patrimonio_liquido)

        giro_do_ativo = (df_dre[i][1] / df_bal[i][35])
        giro_at.append(giro_do_ativo)

        margem_liquida = (df_dre[i][39] / df_dre[i][1])
        marg_liq.append(margem_liquida)

        retorno_sobre_os_ativos = (df_dre[i][39] / df_bal[i][35])
        roa.append(retorno_sobre_os_ativos)

        retorno_sobre_o_patrimonio_liquido = (df_dre[i][39] / df_bal[i][78])
        roe.append(retorno_sobre_o_patrimonio_liquido)

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
        'ROE/RSPL': roe
    }, index=['2017', '2018', '2019', '2020', '2021'])

    df_com_indicadores.to_parquet(output_file)
