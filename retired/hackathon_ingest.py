#!/usr/bin/env python
# coding: utf-8

# #Ingestão Base 2

import pandas as pd
from sqlalchemy import create_engine

file_path = "/content/Base 2 - Vendas Clientes ConsigCar.xlsx"


df = pd.read_excel(file_path)

# Remove 'R$', pontos e converte a vírgula decimal para ponto
df['Valor parcela'] = df['Valor parcela'].replace({'R\$': '', '\.': '', ',': '.'}, regex=True).astype(float)

# Converte a coluna de data e remove a hora
df['Data do Pagamento'] = pd.to_datetime(df['Data do Pagamento']).dt.date

# Cria conexão com banco de dados SQLite
engine = create_engine("sqlite:///vendas_clientes_consigcar.db")

# Insere a tabela no banco
df.to_sql("vendas_clientes_consigcar", con=engine, if_exists="replace", index=False)


#--------------------------------------------------------------------------------
# #Ingestão Base 1


#função para editar a tabela de Receita
def edit_sheet(df):
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.columns = df.columns.str.replace('\n', '_')
    df.columns = df.columns.str.replace(' ', '_')

    return df

#--------------------------------------------------------------------------------
import requests
import pandas as pd
from io import BytesIO

#Definindo ID da planilha e url
sheet_id = '1cucnW4yVosO5n5BFgwXYv6rVy8yj6NTasM83RTCMOug'
sheet_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx'

#Salvando arquivo em FILE
response = requests.get(sheet_url)
file = BytesIO(response.content)

#Lendo e aramazenando em um dataframe as tabelas
df_Receita = pd.read_excel(file, sheet_name='Receita', index_col=False)
df_Receita = edit_sheet(df_Receita)
df_Despesas = pd.read_excel(file, sheet_name='Despesas', index_col=False)

#Inciano tratamento
#exclusão de linhas sem dados nas colunas Data e Mês
df_Receita = df_Receita.dropna(subset=["Data", "Mes"])

#Transformação da cluna data
df_Receita['Data'] = pd.to_datetime(df_Receita['Data']).dt.date

#transformação das colunas Mês e Ano para inteiro
df_Receita["Mes"] = df_Receita["Mes"].astype(int)
df_Receita["Ano"] = df_Receita["Ano"].astype(int)

#colunas = df_Receita.columns

#Separação das colunas da Alucar e Consigcar
df_Receita_Alucar = df_Receita.iloc[:, 0:5] \
                    .reset_index(drop = True)

df_Receita_ConsigCar = df_Receita.iloc[:, [1,6]] \
                        .dropna(subset=["Valor"]) \
                        .reset_index(drop = True)

#Separação do dataframe da Receita da Alucar e estimativa da Alucar
df_Receita_Alucar_estimativa = df_Receita_Alucar[df_Receita_Alucar["Nome_(Alucar)"] == 'Estimativa'] \
                                .reset_index(drop = True)

df_Receita_Alucar = df_Receita_Alucar[df_Receita_Alucar["Nome_(Alucar)"] != 'Estimativa']


#--------------------------------------------------------------------------------
# #Ingestão da planilha despesas


#Excluindo colunas desnecessárias
df_Despesas.columns = df_Despesas.iloc[0]
df_Despesas = df_Despesas[1:]
df_Despesas.reset_index(drop = True, inplace = True)


#Separando Despesas da Alucar e ConsigCar

# Encontrar o índice da linha que contém TOTAL na coluna Despesa
index_total = df_Despesas[df_Despesas['DESPESAS'].str.contains("TOTAL", na=False)].index.min()

# Selecionar todas as linhas até a linha anterior à que contém TOTAL
df_Despesas_Alucar = df_Despesas.iloc[:index_total].copy()
df_Despesas_Alucar = df_Despesas_Alucar.reset_index(drop = True) \
                                       .melt(id_vars=["DESPESAS"], var_name="Mês", value_name="Valor")

# Selecionar as linhas referentes a ConsigCar
df_Despesas_ConsigCar = df_Despesas.iloc[index_total+4:-1] \
                        .copy().reset_index(drop = True) \
                        .melt(id_vars=["DESPESAS"], var_name="Mês", value_name="Valor")





