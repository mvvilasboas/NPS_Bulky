import datetime as dt
from datetime import date
import os
import sys
import numpy as np
import pandas as pd
import consultas_prazos as consultas
from consultas_prazos import prazo_viagem

import sqlalchemy
import pymysql
import time, datetime
from datetime import datetime, timedelta
from datetime import timedelta, date
import time


def dia_util(d1, d2):
    cal = BrazilBankCalendar()
    cal.include_ash_wednesday = False
    a = d1
    b = d2
    if pd.isnull(d1) or pd.isnull(d2):
        return np.nan
    else:
        a = pd.to_datetime(d1, infer_datetime_format=True, dayfirst=True)
        b = pd.to_datetime(d2, infer_datetime_format=True, dayfirst=True)
        if b.weekday() in [5, 6]:
            if b > a:
                resp = (cal.get_working_days_delta(a, b)) + 1
            else:
                resp = (cal.get_working_days_delta(a, b))
                resp = resp * -1
        else:
            resp = (cal.get_working_days_delta(a, b))
            if a > b:
                return -resp
    return resp


def flag_atraso_coletas(aux_coleta):
    if aux_coleta > 0:
        return 1
    else:
        return 0


def flag_recompra(pedido):
    if pedido[0] == 'z':
        return 1
    else:
        return 0

def flag_atraso_tp(data_limite_tp, data_entregue):
    if pd.isnull(data_limite_tp):
        return np.nan
    if pd.isnull(data_entregue):
        return np.nan
    if str(data_limite_tp) and str(data_entregue) > str(date.today()):
        return "1"
    if str(data_limite_tp) < str(data_entregue):
        return "1"
    else:
        return "0"


def flag_atraso_fornecedor(data_limite_fornecedor, data_bip):
    if pd.isnull(data_limite_fornecedor):
        return np.nan
    if pd.isnull(data_bip):
        return np.nan
    if str(data_limite_fornecedor) and str(data_bip) > str(date.today()):
        return "1"
    if str(data_limite_fornecedor) < str(data_bip):
        return "1"
    else:
        return "0"

def flag_atraso_coleta(data_limite_coleta, data_expedicao):
    if pd.isnull(data_limite_coleta):
        return np.nan
    if pd.isnull(data_expedicao):
        return np.nan
    if str(data_limite_coleta) and str(data_expedicao) > str(date.today()):
        return "1"
    if str(data_limite_coleta) < str(data_expedicao):
        return "1"
    else:
        return "0"

def flag_atraso_cliente(data_limite_cliente, data_entregue):
    if pd.isnull(data_limite_cliente):
        return np.nan
    if pd.isnull(data_entregue):
        return np.nan
    if str(data_limite_cliente) and str(data_entregue) > str(date.today()):
        return "1"
    if str(data_limite_cliente) < str(data_entregue):
        return "1"
    else:
        return "0"

def flag_diarias(cont_diarias):
    if cont_diarias ==1:
        return 1
    else:
        return 0

def atraso_redespacho(data_expedicao, data_limite_redespacho):
    if pd.isnull(data_limite_redespacho):
        return np.nan
    if pd.isnull(data_expedicao):
        return np.nan
    elif data_expedicao > data_limite_redespacho:
        return "1"


# def atraso_viagem():
#     if

def loucura_loucura(inicio, fim, nome_arquiv):
    ####USAR O SEU USUARIO NÃO O MEU PF VLW
    redshift_endpoint1 = "redshift.madeiramadeira.com.br"
    redshift_user1 = "lucas.valdomiro"
    redshift_pass1 = "UpC59WbbUzkh"
    port1 = 5439
    dbname1 = "wood"

    from sqlalchemy import create_engine
    from sqlalchemy import text
    engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" % (
        redshift_user1, redshift_pass1, redshift_endpoint1, port1, dbname1)
    engine1 = create_engine(engine_string)

    sql3 = f'''
            select 
            bt.pedido,
            bt.nf_madeira,
            bt.sku,
            bt.nf_fornecedor,
            bt.data_expedicao_fornecedor,
            bt.data_entregue,
            bt.data_limite_last_mile,
            bt.data_limite_cliente,
            bt.data_compra,
            bt.data_bip,
            bt.data_aceite_oc,
            bt.data_limite_fornecedor,
            bt.data_faturamento,
            bt.data_redespacho,
            bt.origem,
            bt.data_limite_redespacho,
            bt.transportadora_nota,
            bt.transportadora_entrega,
            pcr.prazo_interno 					as prazo_interno_redes,
            pcr.prazo 							as prazo_redes,
            pcr.prazo_adicional 				as prazo_adicional_redes,
            pcd.prazo_interno 					as prazo_interno_diretas,
            pcd.prazo 							as prazo_diretas,
            pcd.prazo_adicional 				as prazo_adicional_diretas
            from 
            lake_bases_bulkylog.base_transporte bt  
            left join 
            lake_brain.portal_transportadora_nota_fiscal_madeira ptnfm on ptnfm.numero = bt.nf_madeira 
            left join 
            lake_brain.portal_faturamento pf on pf.id = ptnfm.idfk_faturamento 
            left join 
            lake_bases_bulkylog.prazos_cadastrados_redespacho pcr on pcr.id_faturamento = pf.id 
            left join 
            lake_bases_bulkylog.prazos_cadastrados_diretas pcd  on pcd.id = pf.id 
            where 
            bt.data_entregue BETWEEN date('{inicio}') and date('{fim}') 
        '''

    ######ENGINE DO REDSHIFT
    from sqlalchemy import create_engine
    from sqlalchemy import text
    engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" % (
        redshift_user1, redshift_pass1, redshift_endpoint1, port1, dbname1)
    engine3 = create_engine(engine_string)

    df_red3 = pd.read_sql_query(text(sql3), engine3)

    ##Copiando a tabela
    df_ff = df_red3.copy()
    root_path = r'D:\teste_visao_corr\bases'
    ###tratando duplicadas
    def chave_ped_nota(nota, pedido):
        return nota + pedido

    df_ff['chave_pednota'] = df_ff.apply(
        lambda row: chave_ped_nota(str(row['nf_madeira']), row['pedido']), axis=1)
    df_ff = df_ff.drop_duplicates(subset='chave_pednota', keep='first')

    df_ff['chave_viagem'] = df_ff.apply(
        lambda row: chave_ped_nota(row['transportadora_nota'], row['origem']), axis=1)

    ###merge prazo viagem
    pz_viagem = pd.read_csv(r'D:\teste_visao_corr\prazo_viagem\prazo_viagematualizado.csv',sep=";")
    df_ff = df_ff.merge(pz_viagem, left_on='chave_viagem', right_on='transp_origem', how='left')


    ###regras_flag

    df_ff['flag_atraso_fornecedor'] = df_ff.apply(
        lambda row: flag_atraso_fornecedor(row['data_limite_fornecedor'], row['data_bip']), axis=1)

    df_ff['flag_atraso_transporte'] = df_ff.apply(
        lambda row: flag_atraso_tp(row['data_limite_last_mile'], row['data_entregue']), axis=1)

    df_ff['flag_atraso_coleta'] = df_ff.apply(
        lambda row: flag_atraso_coleta(row['data_limite_fornecedor'], row['data_expedicao_fornecedor']), axis=1)

    df_ff['flag_atraso_redespacho'] = df_ff.apply(
        lambda row: atraso_redespacho(row['data_expedicao_fornecedor'], row['data_limite_redespacho']), axis=1)

    df_ff.head()
    ##atraso_viagem
    ##atraso_chao
    ##diaria


 


    df_ff = df_ff.drop_duplicates(subset='chave_pednota', keep='first')

    ####jogando em um excel, escolher a pasta aonde irá salvar e renomear o arquivo]
    df_ff.to_csv(os.path.join(root_path, "base_sla_transp_" + nome_arquiv + ".csv"), sep=";", index=False)
    # df_ff.to_excel(r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\56 - Qualidade Atendimento\888 - Analise_atraso\base_fechados\base_fechados09.xlsx', index=False)

    return df_ff



def rodar_loop_batota():
    lista = [['20220801', '20220831', '08'],
            ['20220701', '20220731', '07'],
            ['20220601', '20220630', '06']]
            # ['20220501', '20220530', '05'],
            #  ['20220401', '20220430', '04'],
            #  ['20220301', '20220330', '03'],
            #  ['20220201', '20220230', '02'],
    #         #  ['20220101', '20220130', '01']]
    #         [['20221001', '20221030', '10'],
    # #         ['20220901', '20220930', '09']]

    inicio_base = datetime.now()

    l = 0
    for list in lista:
        print(list)
        try:
            print('atualizando mês: ', lista[l][2])
            loucura_loucura(lista[l][0],lista[l][1],lista[l][2])
            l = l + 1
        except Exception as e:
            print('erro: ', e)
            l = l + 1
    print("duração total: "," %s" % (datetime.now() - inicio_base))
    print('---')



def main():

    # roda a bases
    rodar_loop_batota()


main()

