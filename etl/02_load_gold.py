#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Bibliotecas

import pandas as pd
import os
from sqlalchemy import create_engine, inspect, text, Integer, Float, Text
from io import BytesIO
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from utils import sql_to_dbml


# In[2]:


# Dicionários e variáveis
db_path_silver = "../database/silver/01_silver.db"
db_path_gold = "../database/gold/02_gold.db"


# ## Criação das tabelas

# In[3]:


# Cria conexão com banco de dados SQLite
engine_gold = create_engine(f"sqlite:///{db_path_gold}")
engine_silver = create_engine(f"sqlite:///{db_path_silver}")


# In[4]:


# Criação das tabelas
create_scripts = [
"""
-- Visão mensal consolidada de lucro e margem (TOTAL)
CREATE TABLE IF NOT EXISTS g_fato_financas_mensal (
  id_data INTEGER NOT NULL, -- Referência à data representando o mês na dim_tempo
  receita_total DECIMAL(10,2),     -- Receita somada de todas as fontes no mês
  despesas_total DECIMAL(10,2),    -- Total de despesas no mês
  lucro DECIMAL(10,2),            -- Lucro no mês (receita - despesas)
  margem_lucro DECIMAL(10,2),     -- Margem de lucro no mês (lucro / receita)
  evolucao_lucro DECIMAL(10,2)    -- Comparação percentual com o mês anterior (lucro atual / lucro anterior - 1)
);
""",
"""
-- Visão mensal consolidada de lucro e margem (ALUCAR)
CREATE TABLE IF NOT EXISTS g_fato_financas_mensal_alucar (
  id_data INTEGER NOT NULL, -- Referência à data representando o mês na dim_tempo
  receita_total DECIMAL(10,2),     -- Total de receita gerada pelas vendas da Alucar no mês
  despesas_total DECIMAL(10,2),    -- Despesas atribuídas à operação da Alucar no mês
  lucro DECIMAL(10,2),            -- Lucro mensal da Alucar (receita - despesas)
  margem_lucro DECIMAL(10,2),     -- Margem de lucro da Alucar (lucro / receita)
  evolucao_lucro DECIMAL(10,2)    -- Comparação com o mês anterior (lucro atual / lucro anterior - 1)
);
""",
"""
-- Visão mensal consolidada de lucro e margem (CONSIGCAR)
CREATE TABLE IF NOT EXISTS g_fato_financas_mensal_consigcar (
  id_data INTEGER NOT NULL, -- Referência à data representando o mês na dim_tempo
  receita_total DECIMAL(10,2),     -- Total de receita gerada pelas vendas da Consigcar no mês
  despesas_total DECIMAL(10,2),    -- Despesas atribuídas à operação da Consigcar no mês
  lucro DECIMAL(10,2),            -- Lucro mensal da Consigcar (receita - despesas)
  margem_lucro DECIMAL(10,2),     -- Margem de lucro da Consigcar (lucro / receita)
  evolucao_lucro DECIMAL(10,2),   -- Comparação com o mês anterior (lucro atual / lucro anterior - 1)
  faturamento_pagseguro DECIMAL(10,2) -- Receita do PagSeguro no mês
);
""",
"""
-- Visão anual consolidada da empresa como um todo (Alucar + Consigcar)
CREATE TABLE IF NOT EXISTS g_fato_financas_anual (
  id_data INTEGER NOT NULL, -- Referência a uma data do ano na dim_tempo (pode be 01/jan)
  receita_total DECIMAL(10,2),     -- Receita total combinada de todas as operações no ano
  despesas_total DECIMAL(10,2),    -- Despesas totais combinadas no ano
  lucro DECIMAL(10,2),            -- Lucro consolidado no ano (receita - despesas)
  margem_lucro DECIMAL(10,2),     -- Margem de lucro anual consolidada
  evolucao_lucro DECIMAL(10,2)    -- Comparação com o ano anterior (lucro atual / lucro anterior - 1)
);
""",
"""
-- Visão anual da operação da empresa Alucar
CREATE TABLE IF NOT EXISTS g_fato_financas_anual_alucar (
  id_data INTEGER NOT NULL, -- Referência a uma data do ano na dim_tempo
  receita_total DECIMAL(10,2),     -- Receita total gerada pela Alucar no ano
  despesas_total DECIMAL(10,2),    -- Despesas totais da Alucar no ano
  lucro DECIMAL(10,2),            -- Lucro anual da Alucar (receita - despesas)
  margem_lucro DECIMAL(10,2),     -- Margem de lucro anual da Alucar
  evolucao_lucro DECIMAL(10,2)    -- Comparação com o ano anterior da Alucar
);
""",
"""
-- Visão anual da operação da empresa Consigcar
CREATE TABLE IF NOT EXISTS g_fato_financas_anual_consigcar (
  id_data INTEGER NOT NULL, -- Referência a uma data do ano na dim_tempo
  receita_total DECIMAL(10,2),     -- Receita total gerada pela Consigcar no ano
  despesas_total DECIMAL(10,2),    -- Despesas totais da Consigcar no ano
  lucro DECIMAL(10,2),            -- Lucro anual da Consigcar (receita - despesas)
  margem_lucro DECIMAL(10,2),     -- Margem de lucro anual da Consigcar
  evolucao_lucro DECIMAL(10,2)    -- Comparação com o ano anterior da Consigcar
);
""",
"""
-- DRE simplificada com categorias
CREATE TABLE IF NOT EXISTS g_dre_despesas (
  id_data INTEGER NOT NULL PRIMARY KEY, -- Chave primária referenciando a data
  ano INTEGER,                        -- Ano da competência
  mes INTEGER,                        -- Mês da competência
  categoria TEXT,                    -- Categoria contábil da despesa ou receita, ex: Receita Bruta, Impostos, Custo, Despesas Operacionais
  origem TEXT,                      -- Origem da despesa/receita
  tipo TEXT,                        -- Indica se é Receita ou Despesa
  valor DECIMAL(10,2)                       -- Valor financeiro da linha
);
""",
"""
-- PLR - Vendas por vendedor com ranking mensal
CREATE TABLE IF NOT EXISTS g_plr_vendas_vendedor_mensal (
  id_vendedor INTEGER NOT NULL,        -- Referência ao vendedor
  id_data INTEGER NOT NULL,             -- Referência à data representando o mês
  total_vendas INTEGER,                 -- Quantidade de vendas realizadas no mês
  valor_parcelas_total DECIMAL(10,2),          -- Soma dos valores de parcelas (não o total das vendas)
  ranking INTEGER,                    -- Posição no ranking de vendas mensal, baseado em qtd e valor das parcelas
  PRIMARY KEY (id_vendedor, id_data)
);
""",
"""
-- PLR - Vendas por vendedor com ranking nos últimos 10 dias de cada mês
CREATE TABLE IF NOT EXISTS g_plr_vendas_ultimos_10_dias (
  id_data INTEGER NOT NULL,             -- Data de referência, geralmente o último dia do mês
  id_vendedor INTEGER NOT NULL,         -- Referência ao vendedor na dim_vendedor
  total_vendas INTEGER,                 -- Número total de vendas feitas pelo vendedor nos últimos 10 dias do mês
  valor_total DECIMAL(10,2),                   -- Soma das parcelas das vendas feitas pelo vendedor nesse período
  ranking INTEGER,                    -- Posição do vendedor no ranking dos últimos 10 dias do mês, considerando total_vendas e valor_total
  PRIMARY KEY (id_vendedor, id_data)
);
""",
"""
-- Vendas diárias por vendedor
CREATE TABLE IF NOT EXISTS g_plr_vendas_vendedor_diaria (
  id_data INTEGER NOT NULL,             -- Data da venda
  id_vendedor INTEGER NOT NULL,         -- Referência ao vendedor
  total_vendas INTEGER,                 -- Número total de vendas no dia
  valor_total DECIMAL(10,2),            -- Valor total das vendas no dia
  nome_vendedor VARCHAR(255),           -- Nome do vendedor
  PRIMARY KEY (id_vendedor, id_data)
);
""",
"""
-- Metas da Consigcar
CREATE TABLE IF NOT EXISTS g_metas_consigcar (
  id_data INTEGER NOT NULL,             -- Data de referência
  meta_vendas_1_cum INTEGER,            -- Meta cumulativa 1
  meta_vendas_2_cum INTEGER,            -- Meta cumulativa 2
  meta_vendas_1_mes INTEGER,            -- Meta mensal 1
  meta_vendas_2_mes INTEGER,            -- Meta mensal 2
  meta_cum_atingida INTEGER,            -- Indicador se meta cumulativa foi atingida
  meta_mes_atingida INTEGER,            -- Indicador se meta mensal foi atingida
  PRIMARY KEY (id_data)
);
""",
"""
-- Metas da Alucar
CREATE TABLE IF NOT EXISTS g_metas_alucar (
  id_data INTEGER NOT NULL,             -- Data de referência
  meta_vendas_1_cum INTEGER,            -- Meta cumulativa 1
  meta_vendas_2_cum INTEGER,            -- Meta cumulativa 2
  meta_vendas_1_mes INTEGER,            -- Meta mensal 1
  meta_vendas_2_mes INTEGER,            -- Meta mensal 2
  meta_cum_atingida INTEGER,            -- Indicador se meta cumulativa foi atingida
  meta_mes_atingida INTEGER,            -- Indicador se meta mensal foi atingida
  PRIMARY KEY (id_data)
);
""",
"""
-- Estimativa de vendas da Alucar
CREATE TABLE IF NOT EXISTS g_fato_vendas_alucar_estimativa (
  id_data BIGINT NOT NULL,              -- Data de referência
  valor_receita_estimativa TEXT,        -- Valor estimado de receita
  PRIMARY KEY (id_data)
);
"""
]

# Executa cada comando separadamente
with engine_gold.begin() as conn:
    for stmt in create_scripts:
        conn.execute(text(stmt))


# # Transformação e preenchimento das colunas

# In[5]:


# g_fato_financas_mensal_alucar
# Tabela contendo dados de financas da empresa Alucar

# Lê apenas meses e anos distintos da tabela dim_tempo da camada silver
df_dim_tempo = pd.read_sql_query("SELECT DISTINCT mes, ano FROM dim_tempo", engine_silver)

# Gera o primeiro dia de cada mês em formato YYYYMMDD
df_primeiro_dia_mes = df_dim_tempo.apply(lambda x: int(f"{x['ano']}{x['mes']:02d}01"), axis=1).to_frame('id_data')

# Cria o DataFrame para g_fato_financas_mensal com apenas id_data
df_fato_financas_mensal_alucar = pd.DataFrame({
    'id_data': df_primeiro_dia_mes['id_data']
})

# Ordena o DataFrame por id_data
df_fato_financas_mensal_alucar = df_fato_financas_mensal_alucar.sort_values('id_data')

# Calcula a receita total por mês a partir das vendas da Alucar
df_receita = pd.read_sql_query("""
    SELECT 
        (CAST(CAST(dt.ano AS TEXT) || substr('0' || CAST(dt.mes AS TEXT), -2) || '01' AS int)) as id_data,
        SUM(fva.valor_venda) as receita_total
    FROM fato_vendas_alucar fva
    JOIN dim_tempo dt ON fva.id_data = dt.id_data 
    GROUP BY dt.ano, dt.mes
""", engine_silver)

# Faz o merge com o DataFrame principal
df_fato_financas_mensal_alucar = df_fato_financas_mensal_alucar.merge(
    df_receita,
    on='id_data',
    how='left'
)

# Calcula o total de despesas por mês para Alucar
df_despesas = pd.read_sql_query("""
    SELECT 
        (CAST(CAST(dt.ano AS TEXT) || substr('0' || CAST(dt.mes AS TEXT), -2) || '01' AS int)) as id_data,
        SUM(fd.valor) as despesas_total
    FROM fato_despesas fd
    JOIN dim_tempo dt ON fd.id_data = dt.id_data
    WHERE fd.origem = 'Alucar'
    GROUP BY dt.ano, dt.mes
""", engine_silver)

# Faz o merge com o DataFrame principal
df_fato_financas_mensal_alucar = df_fato_financas_mensal_alucar.merge(
    df_despesas,
    on='id_data',
    how='left'
)

# Calcula o lucro (receita - despesas)
df_fato_financas_mensal_alucar['lucro'] = df_fato_financas_mensal_alucar['receita_total'] - df_fato_financas_mensal_alucar['despesas_total']

# Calcula a margem de lucro (lucro / receita)
df_fato_financas_mensal_alucar['margem_lucro'] = df_fato_financas_mensal_alucar['lucro'] / df_fato_financas_mensal_alucar['receita_total']

# Calcula a evolução do lucro (variação percentual em relação ao mês anterior)
df_fato_financas_mensal_alucar['evolucao_lucro'] = df_fato_financas_mensal_alucar['lucro'].pct_change(fill_method=None)

# Preenche valores nulos com 0
df_fato_financas_mensal_alucar = df_fato_financas_mensal_alucar.fillna(0)

# Insere os dados na tabela g_fato_financas_mensal_alucar
from sqlalchemy import Integer, Float
df_fato_financas_mensal_alucar.to_sql('g_fato_financas_mensal_alucar', engine_gold, if_exists='replace', index=False, dtype={
    'id_data': Integer(),
    'receita_total': Integer(),
    'despesas_total': Integer(),
    'lucro': Integer(),
    'margem_lucro': Float(),
    'evolucao_lucro': Float()
})

# Filtra apenas datas de 2025 e exibe o DataFrame
#print("\nDados financeiros mensais Alucar 2025:")
#print(df_fato_financas_mensal_alucar[df_fato_financas_mensal_alucar['id_data'].astype(str).str.startswith('2025')].to_string())


# In[6]:


# g_fato_financas_mensal_consigcar
# # Cria DataFrame base com todas as datas
df_fato_financas_mensal_consigcar = pd.read_sql_query("""
    WITH max_data AS (
        SELECT MAX(id_data) as max_id_data 
        FROM dim_pagamentos_programados
    )
    SELECT DISTINCT
        (CAST(CAST(ano AS TEXT) || substr('0' || CAST(mes AS TEXT), -2) || '01' AS BIGINT)) as id_data
    FROM dim_tempo, max_data
    WHERE id_data <= max_data.max_id_data
    ORDER BY id_data
""", engine_silver)

# Calcula a receita total (vendas) por mês para Consigcar
df_receita = pd.read_sql_query("""
    SELECT 
        (CAST(CAST(dt.ano AS TEXT) || substr('0' || CAST(dt.mes AS TEXT), -2) || '01' AS BIGINT)) as id_data,
        SUM(fvc.valor_total) as receita_total
    FROM fato_vendas_consigcar fvc
    JOIN dim_tempo dt ON fvc.data_primeira_parcela = dt.id_data 
    GROUP BY dt.ano, dt.mes
""", engine_silver)


# Faz o merge com o DataFrame principal
df_fato_financas_mensal_consigcar = df_fato_financas_mensal_consigcar.merge(
    df_receita,
    on='id_data',
    how='left'
)

# Calcula o total de despesas por mês para Consigcar
df_despesas = pd.read_sql_query("""
    SELECT 
        (CAST(CAST(dt.ano AS TEXT) || substr('0' || CAST(dt.mes AS TEXT), -2) || '01' AS BIGINT)) as id_data,
        SUM(fd.valor) as despesas_total
    FROM fato_despesas fd
    JOIN dim_tempo dt ON fd.id_data = dt.id_data
    WHERE fd.origem = 'Consigcar'
    GROUP BY dt.ano, dt.mes
""", engine_silver)

# Faz o merge com o DataFrame principal
df_fato_financas_mensal_consigcar = df_fato_financas_mensal_consigcar.merge(
    df_despesas,
    on='id_data',
    how='left'
)

##### Parte referente aos cálculos realizados com coluna valor_faturado (soma do valor das vendas)
# Calcula o lucro (receita - despesas)
df_fato_financas_mensal_consigcar['lucro'] = df_fato_financas_mensal_consigcar['receita_total'] - df_fato_financas_mensal_consigcar['despesas_total']

# Calcula a margem de lucro (lucro / receita)
df_fato_financas_mensal_consigcar['margem_lucro'] = df_fato_financas_mensal_consigcar['lucro'] / df_fato_financas_mensal_consigcar['receita_total']

# Calcula a evolução do lucro (variação percentual em relação ao mês anterior)
df_fato_financas_mensal_consigcar['evolucao_lucro'] = df_fato_financas_mensal_consigcar['lucro'].pct_change(fill_method=None)

# Preenche valores nulos com 0
df_fato_financas_mensal_consigcar = df_fato_financas_mensal_consigcar.fillna(0)

# Calcula o total de parcelamentos a receber por mês para Consigcar a partir de dim_pagamentos_programados
df_parcelamentos = pd.read_sql_query("""
    SELECT 
        (CAST(CAST(dt.ano AS TEXT) || substr('0' || CAST(dt.mes AS TEXT), -2) || '01' AS BIGINT)) as id_data,
        SUM(dpp.valor) as parcelamentos_receber
    FROM dim_pagamentos_programados dpp
    JOIN dim_tempo dt ON dpp.id_data = dt.id_data
    GROUP BY dt.ano, dt.mes
""", engine_silver)

# Faz o merge com o DataFrame principal
df_fato_financas_mensal_consigcar = df_fato_financas_mensal_consigcar.merge(
    df_parcelamentos,
    on='id_data',
    how='left'
)

##### Parte referente aos cálculos realizados com coluna faturamento_pagseguro 

# Puxa os dados de faturamento do PagSeguro (já mensal na camada silver)

df_receita_pagseguro = pd.read_sql_query("""
    SELECT 
        id_data,
        valor_faturado as faturamento_pagseguro
    FROM fato_faturamento_pagseguro
""", engine_silver)

# Faz o merge com o DataFrame principal
df_fato_financas_mensal_consigcar = df_fato_financas_mensal_consigcar.merge(
    df_receita_pagseguro,
    on='id_data',
    how='left'
)

# Preenche valores nulos com 0
df_fato_financas_mensal_consigcar['faturamento_pagseguro'] = df_fato_financas_mensal_consigcar['faturamento_pagseguro'].fillna(0)

# Calcula o lucro baseado no faturamento do PagSeguro (faturamento_pagseguro - despesas)
df_fato_financas_mensal_consigcar['lucro_pag'] = df_fato_financas_mensal_consigcar['faturamento_pagseguro'] - df_fato_financas_mensal_consigcar['despesas_total']

# Calcula a margem de lucro baseada no faturamento do PagSeguro (lucro_pag / faturamento_pagseguro)
df_fato_financas_mensal_consigcar['margem_lucro_pag'] = df_fato_financas_mensal_consigcar['lucro_pag'] / df_fato_financas_mensal_consigcar['faturamento_pagseguro']

# Calcula a evolução do lucro baseado no faturamento do PagSeguro (variação percentual em relação ao mês anterior)
df_fato_financas_mensal_consigcar['evolucao_lucro_pag'] = df_fato_financas_mensal_consigcar['lucro_pag'].pct_change(fill_method=None)

# Preenche valores nulos com 0
df_fato_financas_mensal_consigcar['evolucao_lucro_pag'] = df_fato_financas_mensal_consigcar['evolucao_lucro_pag'].fillna(0)


# Insere os dados na tabela g_fato_financas_mensal_consigcar
df_fato_financas_mensal_consigcar.to_sql('g_fato_financas_mensal_consigcar', engine_gold, if_exists='replace', index=False, dtype={
    'id_data': Integer(),
    'receita_total': Integer(),
    'despesas_total': Integer(),
    'lucro': Integer(),
    'margem_lucro': Float(),
    'evolucao_lucro': Float(),
    'parcelamentos_receber': Integer(),
    'faturamento_pagseguro': Integer(),
    'lucro_pag': Integer(),
    'margem_lucro_pag': Float(),
    'evolucao_lucro_pag': Float()
})

# print(df_fato_financas_mensal_consigcar[df_fato_financas_mensal_consigcar['id_data'].astype(str).str.startswith('2025')])

# Lista todas das colunas e seus tipos do DataFrame
# for col in df_fato_financas_mensal_consigcar.columns:
#  print(f"{col}: {df_fato_financas_mensal_consigcar[col].dtype}")


# In[7]:


# g_fato_financas_mensal
# Lê as tabelas de finanças mensais do Consigcar e Alucar
# Obs.: Essa tabela usa a "Receita" da empresa Consigcar a partir da soma das vendas do mês. 
# Na tabela original, o valor utilizado é o do PagSeguro. Isso precisará ser ajustado caso seja solicitado. 
df_consigcar = pd.read_sql_table('g_fato_financas_mensal_consigcar', engine_gold)
df_alucar = pd.read_sql_table('g_fato_financas_mensal_alucar', engine_gold)

# Faz o merge das duas tabelas usando outer join para pegar todos os meses
df_fato_financas_mensal = pd.merge(
    df_consigcar,
    df_alucar,
    on='id_data',
    how='outer',
    suffixes=('_consigcar', '_alucar')
)

# Preenche valores nulos com 0 para fazer as somas e converte para int
df_fato_financas_mensal = df_fato_financas_mensal.fillna(0)
df_fato_financas_mensal['id_data'] = df_fato_financas_mensal['id_data'].astype('int')

# Soma os valores das duas empresas e converte para int
df_fato_financas_mensal['receita_total'] = (df_fato_financas_mensal['receita_total_consigcar'] + df_fato_financas_mensal['receita_total_alucar']).astype('int')
df_fato_financas_mensal['despesas_total'] = (df_fato_financas_mensal['despesas_total_consigcar'] + df_fato_financas_mensal['despesas_total_alucar']).astype('int')
df_fato_financas_mensal['lucro'] = (df_fato_financas_mensal['lucro_consigcar'] + df_fato_financas_mensal['lucro_alucar']).astype('int')

# Calcula a margem de lucro total (mantém como float)
df_fato_financas_mensal['margem_lucro'] = (df_fato_financas_mensal['lucro'] / df_fato_financas_mensal['receita_total']) * 100

# Calcula a evolução do lucro (mantém como float)
df_fato_financas_mensal = df_fato_financas_mensal.sort_values('id_data')
df_fato_financas_mensal['evolucao_lucro'] = df_fato_financas_mensal['lucro'].pct_change() * 100

# Seleciona apenas as colunas necessárias
df_fato_financas_mensal = df_fato_financas_mensal[[
    'id_data',
    'receita_total',
    'despesas_total',
    'lucro',
    'margem_lucro',
    'evolucao_lucro'
]]

# Insere os dados na tabela g_fato_financas_mensal
df_fato_financas_mensal.to_sql('g_fato_financas_mensal', engine_gold, if_exists='replace', index=False, dtype={
    'id_data': Integer(),
    'receita_total': Integer(),
    'despesas_total': Integer(),
    'lucro': Integer(),
    'margem_lucro': Float(),
    'evolucao_lucro': Float()
})

# Imprime o DataFrame para verificação apenas de 2025
# print(df_fato_financas_mensal[df_fato_financas_mensal['id_data'].astype(str).str.startswith('2025')])


# In[8]:


# g_fato_financas_anual_alucar
# Lê a tabela mensal da Alucar que já foi criada
df_alucar = pd.read_sql_table('g_fato_financas_mensal_alucar', engine_gold)

# Converte id_data para string e pega só o ano (primeiros 4 dígitos)
df_alucar['ano'] = df_alucar['id_data'].astype(str).str[:4]

# Agrupa por ano e soma os valores
df_alucar_anual = df_alucar.groupby('ano').agg({
    'receita_total': 'sum',
    'despesas_total': 'sum',
    'lucro': 'sum'
}).reset_index()

# Converte ano de volta para id_data (ano + "0101")
df_alucar_anual['id_data'] = df_alucar_anual['ano'] + '0101'

# Remove a coluna ano
df_alucar_anual = df_alucar_anual.drop('ano', axis=1)

# Calcula a margem de lucro
df_alucar_anual['margem_lucro'] = (df_alucar_anual['lucro'] / df_alucar_anual['receita_total']) * 100

# Calcula a evolução do lucro
df_alucar_anual = df_alucar_anual.sort_values('id_data')
df_alucar_anual['evolucao_lucro'] = df_alucar_anual['lucro'].pct_change() * 100

# Seleciona e ordena as colunas necessárias
df_alucar_anual = df_alucar_anual[[
    'id_data',
    'receita_total',
    'despesas_total',
    'lucro',
    'margem_lucro',
    'evolucao_lucro'
]]


# Insere os dados na tabela g_fato_financas_anual_alucar
df_alucar_anual.to_sql('g_fato_financas_anual_alucar', engine_gold, if_exists='replace', index=False, dtype={
    'id_data': Integer(),
    'receita_total': Integer(),
    'despesas_total': Integer(),
    'lucro': Integer(),
    'margem_lucro': Float(),
    'evolucao_lucro': Float()
})

# Imprime o DataFrame para verificação
# print("\nDados anuais da Alucar:")
# print(df_alucar_anual)


# In[9]:


# g_fato_financas_anual_consigcar

# Lê a tabela g_fato_financas_mensal_consigcar
df_consigcar = pd.read_sql_table('g_fato_financas_mensal_consigcar', engine_gold)

# Converte id_data para string e pega só o ano (primeiros 4 dígitos)
df_consigcar['ano'] = df_consigcar['id_data'].astype(str).str[:4]

# Agrupa por ano e soma os valores
df_consigcar_anual = df_consigcar.groupby('ano').agg({
    'receita_total': 'sum',
    'despesas_total': 'sum',
    'lucro': 'sum'
}).reset_index()

# Converte ano de volta para id_data (ano + "0101")
df_consigcar_anual['id_data'] = df_consigcar_anual['ano'] + '0101'

# Remove a coluna ano
df_consigcar_anual = df_consigcar_anual.drop('ano', axis=1)

# Calcula a margem de lucro
df_consigcar_anual['margem_lucro'] = (df_consigcar_anual['lucro'] / df_consigcar_anual['receita_total']) * 100

# Calcula a evolução do lucro
df_consigcar_anual = df_consigcar_anual.sort_values('id_data')
df_consigcar_anual['evolucao_lucro'] = df_consigcar_anual['lucro'].pct_change() * 100

# Seleciona e ordena as colunas necessárias
df_consigcar_anual = df_consigcar_anual[[
    'id_data',
    'receita_total',
    'despesas_total',
    'lucro',
    'margem_lucro',
    'evolucao_lucro'
]]

# Insere os dados na tabela g_fato_financas_anual_consigcar
df_consigcar_anual.to_sql('g_fato_financas_anual_consigcar', engine_gold, if_exists='replace', index=False, dtype={
    'id_data': Integer(),
    'receita_total': Integer(),
    'despesas_total': Integer(),
    'lucro': Integer(),
    'margem_lucro': Float(),
    'evolucao_lucro': Float()
})

# Imprime o DataFrame para verificação
# print("\nDados anuais da Consigcar:")
# print(df_consigcar_anual)


# In[10]:


# g_dre_despesas
# Preciso ver como fazer essa tabela
# Lê os dados da tabela fato_despesas da camada silver
df_despesas = pd.read_sql("""
    SELECT *
    FROM fato_despesas
""", engine_silver)

# Converte todas as colunas integer para INT
for col in df_despesas.select_dtypes(include=['int64']).columns:
    df_despesas[col] = df_despesas[col].astype('int')

# Insere os dados na tabela g_dre_despesas
df_despesas.to_sql('g_dre_despesas', engine_gold, if_exists='replace', index=False, dtype={
    'id_data': Integer(),
    'id_despesa': Integer(),
    'id_tipo_despesa': Integer(),
    'valor': Integer()
})


# In[11]:


# g_plr_vendas_vendedor_mensal
# Apaga todos os registros existentes da tabela g_plr_vendas_vendedor_mensal
with engine_gold.connect() as conn:
    conn.execute(text("DELETE FROM g_plr_vendas_vendedor_mensal"))

# Lê os dados da tabela fato_vendas_diaria_vendedor
df_vendas_diarias = pd.read_sql("""
    SELECT 
        id_vendedor,
        id_data,
        total_vendas,
        valor_total
    FROM fato_vendas_diaria_vendedor
""", engine_silver)

# Extrai mês e ano do id_data para agrupar
df_vendas_diarias['mes_ano'] = df_vendas_diarias['id_data'].astype(str).str[:6]

# Agrupa por vendedor e mês, somando os totais
df_vendas_mensais = df_vendas_diarias.groupby(['id_vendedor', 'mes_ano']).agg({
    'total_vendas': 'sum',
    'valor_total': 'sum'
}).reset_index()

# Converte mes_ano para id_data (adiciona "01" ao final)
df_vendas_mensais['id_data'] = df_vendas_mensais['mes_ano'] + '01'
df_vendas_mensais = df_vendas_mensais.drop('mes_ano', axis=1)

# Calcula o ranking mensal baseado no valor total
df_vendas_mensais['ranking'] = df_vendas_mensais.groupby('id_data')['valor_total'].rank(ascending=False, method='dense').astype(int)

# Renomeia a coluna valor_total para valor_parcelas_total para manter consistência com o modelo
df_vendas_mensais = df_vendas_mensais.rename(columns={'valor_total': 'valor_parcelas_total'})

# Insere os dados na tabela g_plr_vendas_vendedor_mensal
df_vendas_mensais.to_sql('g_plr_vendas_vendedor_mensal', engine_gold, if_exists='replace', index=False, dtype={
    'id_data': Integer(),
    'id_vendedor': Integer(),
    'total_vendas': Integer(),
    'valor_parcelas_total': Integer(),
    'ranking': Integer()
})

# print("Dados inseridos com sucesso na tabela g_plr_vendas_vendedor_mensal")
# print(df_vendas_mensais.head())


# In[12]:


# g_plr_vendas_ultimos_10_dias
# Apaga todos os registros existentes da tabela g_plr_vendas_ultimos_10_dias
with engine_gold.connect() as conn:
    conn.execute(text("DELETE FROM g_plr_vendas_ultimos_10_dias"))

# Lê os dados da tabela fato_vendas_diaria_vendedor
df_vendas_diarias = pd.read_sql("""
    SELECT 
        id_vendedor,
        id_data,
        total_vendas,
        valor_total
    FROM fato_vendas_diaria_vendedor
""", engine_silver)

# Converte id_data para datetime
df_vendas_diarias['data'] = pd.to_datetime(df_vendas_diarias['id_data'].astype(str), format='%Y%m%d')

# Calcula o último dia de cada mês
df_vendas_diarias['ultimo_dia_mes'] = df_vendas_diarias['data'] + pd.offsets.MonthEnd(0)

# Filtra apenas os últimos 10 dias de cada mês
df_vendas_ultimos_10_dias = df_vendas_diarias[df_vendas_diarias['data'] >= df_vendas_diarias['ultimo_dia_mes'] - pd.Timedelta(days=9)]

# Agrupa por vendedor e data, somando os totais
df_vendas_agrupadas = df_vendas_ultimos_10_dias.groupby(['id_vendedor', 'id_data']).agg({
    'total_vendas': 'sum',
    'valor_total': 'sum'
}).reset_index()

# Calcula o ranking diário baseado no valor total
df_vendas_agrupadas['ranking'] = df_vendas_agrupadas.groupby('id_data')['valor_total'].rank(ascending=False, method='dense').astype(int)

# Insere os dados na tabela g_plr_vendas_ultimos_10_dias
df_vendas_agrupadas.to_sql('g_plr_vendas_ultimos_10_dias', engine_gold, if_exists='replace', index=False, dtype={
    'id_data': Integer(),
    'id_vendedor': Integer(),
    'total_vendas': Integer(),
    'valor_total': Integer(),
    'ranking': Integer()
})

# print("Dados inseridos com sucesso na tabela g_plr_vendas_ultimos_10_dias")
# print(df_vendas_agrupadas.head())


# In[13]:


# g_plr_vendas_vendedor_diaria
# Carrega dados da tabela fato_vendas_diaria_vendedor da camada Silver
df_vendas_diarias = pd.read_sql("""
    SELECT 
        id_data,
        id_vendedor,
        total_vendas,
        valor_total
    FROM fato_vendas_diaria_vendedor
""", engine_silver)

# Carrega os nomes dos vendedores da tabela dim_vendedor
df_vendedores = pd.read_sql("""
    SELECT 
        id_vendedor,
        nome_vendedor
    FROM dim_vendedor
""", engine_silver)

# Junta as tabelas para obter o nome do vendedor
df_vendas_diarias = df_vendas_diarias.merge(df_vendedores, on='id_vendedor', how='left')

# Insere os dados na tabela g_plr_vendas_vendedor_diaria
df_vendas_diarias.to_sql('g_plr_vendas_vendedor_diaria', engine_gold, if_exists='replace', index=False, dtype={
    'id_data': Integer(),
    'id_vendedor': Integer(),
    'nome_vendedor': Text(),
    'total_vendas': Integer(),
    'valor_total': Integer()
})


# In[14]:


# g_metas_consigcar
# Carrega dados da tabela fato_metas_consigcar da camada Silver
df_metas = pd.read_sql("""
    SELECT 
        id_data,
        meta_vendas_1_cum,
        meta_vendas_2_cum,
        meta_vendas_1_mes,
        meta_vendas_2_mes
    FROM fato_metas_consigcar
""", engine_silver)

# Carrega dados de vendas para contar quantas vendas foram feitas
df_vendas = pd.read_sql("""
    SELECT 
        data_primeira_parcela AS id_data,
        id_venda_consigcar
    FROM fato_vendas_consigcar
""", engine_silver)

# Converte id_data para string em ambos DataFrames para garantir compatibilidade
df_metas['id_data'] = df_metas['id_data'].astype(str)
df_vendas['id_data'] = df_vendas['id_data'].astype(str)

# Conta quantidade de vendas mensais
vendas_mensais = df_vendas.groupby('id_data')['id_venda_consigcar'].count().reset_index()
vendas_mensais.columns = ['id_data', 'vendas_mes']

# Calcula vendas cumulativas desde janeiro
df_metas['ano'] = df_metas['id_data'].str[:4]
vendas_cum = df_vendas.copy()
vendas_cum['ano'] = vendas_cum['id_data'].str[:4]
vendas_cum['mes'] = vendas_cum['id_data'].str[4:6]

# Para cada ano, ordena por mês e calcula cumulativo desde janeiro
vendas_cum = vendas_cum.groupby(['ano', 'mes'])['id_venda_consigcar'].count().reset_index()
vendas_cum = vendas_cum.sort_values(['ano', 'mes'])
vendas_cum['vendas_cum'] = vendas_cum.groupby('ano')['id_venda_consigcar'].cumsum()

# Recria id_data para join
vendas_cum['id_data'] = vendas_cum['ano'] + vendas_cum['mes'] + '01'
vendas_cum = vendas_cum[['id_data', 'ano', 'vendas_cum']]

# Junta os dados
df_metas = df_metas.merge(vendas_mensais, on='id_data', how='left')
df_metas = df_metas.merge(vendas_cum, on=['id_data', 'ano'], how='left')

# Preenche colunas com 0 onde houver NA
df_metas['vendas_mes'] = df_metas['vendas_mes'].fillna(0)
df_metas['vendas_cum'] = df_metas['vendas_cum'].fillna(0)

# Calcula meta batida mes
df_metas['meta_cum_atingida'] = 0
df_metas.loc[(df_metas['vendas_cum'] >= df_metas['meta_vendas_1_cum']) & 
             (df_metas['vendas_cum'] < df_metas['meta_vendas_2_cum']), 'meta_cum_atingida'] = 1
df_metas.loc[df_metas['vendas_cum'] >= df_metas['meta_vendas_2_cum'], 'meta_cum_atingida'] = 2

# Calcula meta batida cum
df_metas['meta_mes_atingida'] = 0
df_metas.loc[(df_metas['vendas_mes'] >= df_metas['meta_vendas_1_mes']) & 
             (df_metas['vendas_mes'] < df_metas['meta_vendas_2_mes']), 'meta_mes_atingida'] = 1
df_metas.loc[df_metas['vendas_mes'] >= df_metas['meta_vendas_2_mes'], 'meta_mes_atingida'] = 2

# Seleciona e ordena colunas conforme tabela gold
df_metas = df_metas[[
    'id_data',
    'meta_vendas_1_cum',
    'meta_vendas_2_cum',
    'meta_vendas_1_mes',
    'meta_vendas_2_mes',
    'vendas_mes',
    'vendas_cum',
    'meta_cum_atingida',
    'meta_mes_atingida'
]]

# Insere os dados na tabela g_metas_consigcar
df_metas.to_sql('g_metas_consigcar', engine_gold, if_exists='replace', index=False, dtype={
    'id_data': Integer(),
    'meta_vendas_1_cum': Integer(),
    'meta_vendas_2_cum': Integer(),
    'meta_vendas_1_mes': Float(),
    'meta_vendas_2_mes': Float(),
    'vendas_mes': Integer(),
    'vendas_cum': Integer(),
    'meta_cum_atingida': Integer(),
    'meta_mes_atingida': Integer()
})


# In[15]:


# g_metas_alucar
# Carrega dados da tabela fato_metas_alucar da camada Silver
df_metas = pd.read_sql("""
    SELECT 
        id_data,
        meta_vendas_1_cum,
        meta_vendas_2_cum,
        meta_vendas_1_mes,
        meta_vendas_2_mes
    FROM fato_metas_alucar
""", engine_silver)

# Calcula meta batida mes
df_metas['meta_cum_atingida'] = 0
#df_metas.loc[(df_metas['vendas_cum'] >= df_metas['meta_vendas_1_cum']) & 
#             (df_metas['vendas_cum'] < df_metas['meta_vendas_2_cum']), 'meta_cum_atingida'] = 1
#df_metas.loc[df_metas['vendas_cum'] >= df_metas['meta_vendas_2_cum'], 'meta_cum_atingida'] = 2

# Calcula meta batida cum
df_metas['meta_mes_atingida'] = 0
#df_metas.loc[(df_metas['vendas_mes'] >= df_metas['meta_vendas_1_mes']) & 
#             (df_metas['vendas_mes'] < df_metas['meta_vendas_2_mes']), 'meta_mes_atingida'] = 1
#df_metas.loc[df_metas['vendas_mes'] >= df_metas['meta_vendas_2_mes'], 'meta_mes_atingida'] = 2

# Preenche colunas com 0 conforme solicitado
df_metas['vendas_mes'] = 0
df_metas['vendas_cum'] = 0

# Seleciona e ordena colunas conforme tabela gold
df_metas = df_metas[[
    'id_data',
    'meta_vendas_1_cum',
    'meta_vendas_2_cum',
    'meta_vendas_1_mes',
    'meta_vendas_2_mes',
    'vendas_mes',
    'vendas_cum',
    'meta_cum_atingida',
    'meta_mes_atingida'
]]

# Insere os dados na tabela g_metas_alucar
df_metas.to_sql('g_metas_alucar', engine_gold, if_exists='replace', index=False, dtype={
    'id_data': Integer(),
    'meta_vendas_1_cum': Integer(),
    'meta_vendas_2_cum': Integer(),
    'meta_vendas_1_mes': Float(),
    'meta_vendas_2_mes': Float(),
    'vendas_mes': Integer(),
    'vendas_cum': Integer(),
    'meta_cum_atingida': Integer(),
    'meta_mes_atingida': Integer()
})


# In[16]:


# g_fato_vendas_alucar_estimativa

# Carrega dados da tabela silver fato_vendas_clientes_alucar_estimativa
df_estimativa = pd.read_sql_table('fato_vendas_clientes_alucar_estimativa', engine_silver)

# Seleciona e renomeia colunas conforme tabela gold
df_estimativa = df_estimativa[[
    'id_data',
    'valor_receita_estimativa'
]]

# Insere os dados na tabela g_fato_vendas_alucar_estimativa
df_estimativa.to_sql('g_fato_vendas_alucar_estimativa', engine_gold, if_exists='replace', index=False, dtype={
    'id_data': Integer(),
    'valor_receita_estimativa': Float()
})


# In[17]:


# g_fato_consigcar_estimativa
# Carrega dados da tabela silver fato_consigcar_estimativa
df_estimativa_consigcar = pd.read_sql_table('fato_consigcar_estimativa', engine_silver)

# Seleciona e renomeia colunas conforme tabela gold
df_estimativa_consigcar = df_estimativa_consigcar[[
    'id_data',
    'valor_receita_estimativa'
]]

# Insere os dados na tabela g_fato_consigcar_estimativa
df_estimativa_consigcar.to_sql('g_fato_consigcar_estimativa', engine_gold, if_exists='replace', index=False, dtype={
    'id_data': Integer(),
    'valor_receita_estimativa': Float()
})


# In[18]:


# Transformar o banco de dados em DBML para acompanhamento da modelagem

dbml = sql_to_dbml(engine_gold)
with open("../modelagem/02_gold.dbml", "w", encoding="utf-8") as f:
    f.write(dbml)

