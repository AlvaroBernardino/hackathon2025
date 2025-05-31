#!/usr/bin/env python
# coding: utf-8

# In[84]:


# Bibliotecas

import pandas as pd
import os
from sqlalchemy import create_engine, inspect, text
from io import BytesIO
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from utils import sql_to_dbml
from utils import classificar_despesa
from utils import convert_iddata


# In[85]:


# Dicionários e variáveis
db_path_silver = "../database/silver/01_silver.db"
dbml_path = "../modelagem/01_silver.dbml"


# ## Criação das tabelas

# In[86]:


# Cria conexão com banco de dados SQLite
engine = create_engine(f"sqlite:///{db_path_silver}")


# In[87]:


# Cria tabelas
create_scripts = [
    """
    CREATE TABLE IF NOT EXISTS dim_tempo (
        id_data INTEGER PRIMARY KEY,
        data DATE,
        dia TINYINT,
        mes TINYINT,
        ano SMALLINT,
        nome_mes VARCHAR(15),
        dia_da_semana VARCHAR(10),
        trimestre TINYINT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_cliente (
        id_cliente INTEGER PRIMARY KEY,
        nome_cliente TEXT,
        whatsapp VARCHAR(20)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_vendedor (
        id_vendedor INTEGER PRIMARY KEY,
        nome_vendedor TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_parcelamento (
        id_parcelamento INTEGER PRIMARY KEY,
        num_parcelas SMALLINT,
        valor_parcela DECIMAL(10,2),
        valor_total DECIMAL(10,2),
        data_primeira_parcela INTEGER,
        data_ultima_parcela INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fato_vendas_alucar (
        id_venda_alucar INTEGER PRIMARY KEY,
        id_cliente INTEGER,
        id_data INTEGER,
        valor_venda DECIMAL(10,2),
        FOREIGN KEY(id_cliente) REFERENCES dim_cliente(id_cliente),
        FOREIGN KEY(id_data) REFERENCES dim_tempo(id_data)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fato_vendas_consigcar (
        id_venda_consigcar INTEGER PRIMARY KEY,
        id_cliente INTEGER,
        tipo_produto VARCHAR(20),
        id_parcelamento INTEGER,
        id_vendedor INTEGER,
        id_data INTEGER,
        FOREIGN KEY(id_cliente) REFERENCES dim_cliente(id_cliente),
        FOREIGN KEY(id_parcelamento) REFERENCES dim_parcelamento(id_parcelamento),
        FOREIGN KEY(id_vendedor) REFERENCES dim_vendedor(id_vendedor),
        FOREIGN KEY(id_data) REFERENCES dim_tempo(id_data)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fato_despesas (
        id_despesa INTEGER PRIMARY KEY,
        origem TEXT,
        categoria TEXT,
        nome_despesa TEXT,
        valor DECIMAL(10,2),
        id_data INTEGER,
        FOREIGN KEY(id_data) REFERENCES dim_tempo(id_data)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fato_faturamento_pagseguro (
        id_faturamento INTEGER PRIMARY KEY,
        id_data INTEGER,
        valor_faturado DECIMAL(10,2),
        FOREIGN KEY(id_data) REFERENCES dim_tempo(id_data)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fato_vendas_diaria_vendedor (
        id_vendedor INTEGER NOT NULL,
        id_data INTEGER NOT NULL,
        total_vendas SMALLINT NOT NULL,
        valor_total DECIMAL(10,2) NOT NULL,
        PRIMARY KEY (id_vendedor, id_data),
        FOREIGN KEY (id_vendedor) REFERENCES dim_vendedor(id_vendedor),
        FOREIGN KEY (id_data) REFERENCES dim_tempo(id_data)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_pagamentos_programados (
        id_venda_consigcar BIGINT,
        valor FLOAT,
        id_data BIGINT,
        PRIMARY KEY (id_venda_consigcar, id_data),
        FOREIGN KEY (id_venda_consigcar) REFERENCES fato_vendas_consigcar(id_venda_consigcar)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fato_metas_alucar (
        id_data INTEGER PRIMARY KEY,
        meta_vendas_1_cum INTEGER,
        meta_vendas_1_mes INTEGER,
        meta_vendas_2_cum INTEGER,
        meta_vendas_2_mes INTEGER,
        FOREIGN KEY(id_data) REFERENCES dim_tempo(id_data)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fato_metas_consigcar (
        id_data INTEGER PRIMARY KEY,
        meta_vendas_1_cum INTEGER,
        meta_vendas_1_mes INTEGER,
        meta_vendas_2_cum INTEGER,
        meta_vendas_2_mes INTEGER,
        FOREIGN KEY(id_data) REFERENCES dim_tempo(id_data)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fato_vendas_clientes_alucar_estimativa (
        id_data INTEGER PRIMARY KEY,
        valor_receita_estimativa DECIMAL(10,2),
        FOREIGN KEY(id_data) REFERENCES dim_tempo(id_data)
    );
    """
]

# Executa cada comando separadamente
with engine.begin() as conn:
    for stmt in create_scripts:
        conn.execute(text(stmt))


# In[88]:


# Transformar o banco de dados em DBML para acompanhamento da modelagem
dbml = sql_to_dbml(engine)

with open(dbml_path, "w", encoding="utf-8") as f:
    f.write(dbml)


# ## Carregamento e transformação dos dados da camada Bronze

# In[89]:


# Paths dos bancos Bronze e Silver
db_path_bronze1 = "../database/bronze/00_base1.db"
db_path_bronze2 = "../database/bronze/00_base2.db"
db_path_silver  = "../database/silver/01_silver.db"


# In[90]:


# Engines de conexão
engine_bronze1 = create_engine(f"sqlite:///{db_path_bronze1}")
engine_bronze2 = create_engine(f"sqlite:///{db_path_bronze2}")
engine_silver  = create_engine(f"sqlite:///{db_path_silver}")


# In[91]:


# 1) dim_cliente
# Vem de: 00_vendas_clientes_consigcar e 00_vendas_clientes_alucar
# Unir dados das duas tabelas com empresa de origem e whatsapp quando disponível

# Busca clientes da Consigcar
df_clientes_consig = pd.read_sql(
    'SELECT DISTINCT Nome AS nome_cliente, WhatsApp AS whatsapp '
    'FROM "00_vendas_clientes_consigcar"',
    engine_bronze2
)
df_clientes_consig['empresa'] = 'Consigcar'

# Busca clientes da Alucar 
df_clientes_alu = pd.read_sql(
    'SELECT DISTINCT "Nome_(Alucar)" AS nome_cliente '
    'FROM "00_vendas_clientes_alucar"',
    engine_bronze1
)
df_clientes_alu['whatsapp'] = pd.NA
df_clientes_alu['empresa'] = 'Alucar'

# Concatena os dois dataframes
df_clientes = pd.concat([df_clientes_consig, df_clientes_alu], ignore_index=True)

# Remove duplicatas mantendo a primeira ocorrência
df_clientes = df_clientes.drop_duplicates(subset=['nome_cliente'])

# Salva na tabela dim_cliente
df_clientes.to_sql("dim_cliente", engine_silver, if_exists="replace", index_label="id_cliente")


# In[92]:


# 2) dim_vendedor
# Vem de: 00_base2.db.Vendedor
df_vend = pd.read_sql(
    'SELECT DISTINCT Vendedor AS nome_vendedor '
    'FROM "00_vendas_clientes_consigcar"',
    engine_bronze2
)
df_vend.to_sql("dim_vendedor", engine_silver, if_exists="replace", index_label="id_vendedor")


# In[93]:


# 3) dim_tempo
# Configurar o locale para português
import locale
locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

# Gera calendário de 2018-01-01 a 2030-12-31
start = datetime(2018,1,1)
end   = datetime(2030,12,31)
dates = []
while start <= end:
    dates.append(start)
    start += relativedelta(days=1)

df_tempo = pd.DataFrame({
    "id_data": [int(d.strftime("%Y%m%d")) for d in dates],
    "data": dates,
})
df_tempo["dia"]           = df_tempo["data"].dt.day
df_tempo["mes"]           = df_tempo["data"].dt.month
df_tempo["ano"]           = df_tempo["data"].dt.year
df_tempo["nome_mes"]      = df_tempo["data"].dt.strftime("%B")
df_tempo["dia_da_semana"] = df_tempo["data"].dt.day_name(locale='pt_BR')
df_tempo["trimestre"]     = df_tempo["data"].dt.quarter

df_tempo.to_sql("dim_tempo", engine_silver, if_exists="replace", index=False)


# In[94]:


# 4) fato_despesas
# Vem de: 00_base1.db."00_despesas_alucar", "00_despesas_consigcar"

# Leitura dos dados de despesas da Alucar
df_alucar = pd.read_sql(
    'SELECT DESPESAS AS nome_despesa, Valor AS valor, Mês AS mes '
    'FROM "00_despesas_alucar"',
    engine_bronze1
)
df_alucar["origem"] = "Alucar"

# Leitura dos dados de despesas da ConsigCar
df_consig = pd.read_sql(
    'SELECT DESPESAS AS nome_despesa, Valor AS valor, Mês AS mes '
    'FROM "00_despesas_consigcar"',
    engine_bronze1
)
df_consig["origem"] = "Consigcar"

# Junta ambas as tabelas
df_desp = pd.concat([df_alucar, df_consig], ignore_index=True)

# Remove o "R$" da coluna valor, substitui vírgula por ponto e converte para float
df_desp['valor'] = (df_desp['valor']
                    .str.replace('R$', '', regex=False)
                    .str.replace('.', '', regex=False)
                    .str.replace(',', '.', regex=False)
                    .str.strip()
                    .astype(float))

# Dicionário para mapear nomes dos meses em português para números
meses = {
    'janeiro': '01', 'fevereiro': '02', 'março': '03', 'abril': '04',
    'maio': '05', 'junho': '06', 'julho': '07', 'agosto': '08',
    'setembro': '09', 'outubro': '10', 'novembro': '11', 'dezembro': '12'
}

# Função para converter o nome do mês para o formato de data desejado
def converter_mes(mes):
    mes = mes.lower().strip()
    for nome, numero in meses.items():
        if mes.startswith(nome):
            return f"2025-{numero}-01"
    return None

# Converte o mês para a data no formato correto
df_desp['data'] = df_desp['mes'].apply(converter_mes)

# Converte para datetime e depois para o formato YYYYMMDD para id_data
df_desp["id_data"] = pd.to_datetime(df_desp["data"]).dt.strftime("%Y%m%d").astype("Int64")

# Coluna categoria (placeholder)
df_desp['categoria'] = df_desp['nome_despesa'].apply(classificar_despesa)

# Exporta para camada silver
df_desp[["origem", "categoria", "nome_despesa", "valor", "id_data"]].to_sql(
    "fato_despesas", engine_silver, if_exists="replace", index_label="id_despesa"
)

# print(df_desp.iloc[10:20])


# In[95]:


# 5) fato_faturamento_pagseguro
# Vem de: 00_base1.db."00_receita_pagseguro_consigcar"
df_pag = pd.read_sql(
    'SELECT Data AS data, Valor AS valor_faturado '
    'FROM "00_receita_pagseguro_consigcar"',
    engine_bronze1
)

df_pag["id_data"] = (
    pd.to_datetime(df_pag["data"], dayfirst=False)
    .dt.strftime("%Y%m%d")
    .astype("Int64")
)

# Converter valor_faturado to float64
df_pag["valor_faturado"] = (
    df_pag["valor_faturado"]
    .str.replace('R$', '', regex=False)  # Remove R$ symbol
    .str.replace('.', '', regex=False)   # Remove thousands separator
    .str.replace(',', '.', regex=False)  # Replace decimal comma with point
    .str.strip()                         # Remove whitespace
    .astype('float64')                   # Convert to float64
)

# Salvar na tabela fato_faturamento_pagseguro
df_pag[["id_data","valor_faturado"]].to_sql(
    "fato_faturamento_pagseguro", engine_silver, if_exists="replace", index_label="id_faturamento"
)


# In[96]:


# 6) fato_vendas_alucar
# Vem de: 00_base1.db."00_vendas_clientes_alucar"
df_val = pd.read_sql(
    'SELECT rowid AS id_venda_alucar, '
    'Data AS data, '
    '"Valor_Receita" AS valor_venda, '
    '"Nome_(Alucar)" AS nome_cliente '
    'FROM "00_vendas_clientes_alucar"',
    engine_bronze1
)

# Obter id_cliente da dim_cliente baseado no nome_cliente
df_clientes = pd.read_sql('SELECT id_cliente, nome_cliente FROM dim_cliente', engine_silver)
df_val = df_val.merge(df_clientes, on='nome_cliente', how='left')

# Convertendo data para YYYYMMDD
df_val["id_data"] = pd.to_datetime(df_val["data"], format="%Y-%m-%d").dt.strftime("%Y%m%d").astype("Int64")

# Converter valor_venda de string (R$ X.XXX,XX) para float
df_val["valor_venda"] = (df_val["valor_venda"]
    .str.replace('R$', '', regex=False)  # Remove R$
    .str.replace('.', '', regex=False)   # Remove pontos dos milhares
    .str.replace(',', '.', regex=False)  # Substitui vírgula por ponto decimal
    .str.strip()                         # Remove espaços
    .astype(float)                       # Converte para float
    .round(2)                           # Arredonda para 2 casas decimais
)

# Selecionar e salvar colunas relevantes na tabela fato
df_val[["id_venda_alucar", "id_cliente", "id_data", "valor_venda"]].to_sql(
    "fato_vendas_alucar", 
    engine_silver,
    if_exists="replace", 
    index=False
)




# In[97]:


# 7) fato_vendas_consigcar
# Vem de: 00_base2.db."00_vendas_clientes_consigcar"
# Leitura dos dados da camada bronze
df_cons = pd.read_sql(
    'SELECT rowid AS id_venda_consigcar, '
    'Nome AS nome_cliente, '
    '"Tipo Produto" AS tipo_produto, '
    '"Quantidade de vezes" AS num_parcelas, '
    '"Valor parcela" AS valor_parcela, '
    'Vendedor AS nome_vendedor, '
    '"Data do pagamento" AS data_primeira_parcela '
    'FROM "00_vendas_clientes_consigcar"',
    engine_bronze2
)

# Calcular valor_total
df_cons['valor_total'] = df_cons['num_parcelas'] * df_cons['valor_parcela']

# Garantir que data_primeira_parcela esteja como datetime (caso já tenha sido convertida antes)
df_cons['data_primeira_parcela'] = pd.to_datetime(df_cons['data_primeira_parcela'], errors='coerce')

# Calcular data_ultima_parcela: data_primeira_parcela + (num_parcelas - 1) meses
df_cons['data_ultima_parcela'] = df_cons.apply(
    lambda row: row['data_primeira_parcela'] + relativedelta(months=int(row['num_parcelas']) - 1)
    if pd.notnull(row['data_primeira_parcela']) and pd.notnull(row['num_parcelas']) else pd.NaT,
    axis=1
)

# Formatar ambas as datas no final: YYYYMMDD como string, depois Int64
df_cons['data_primeira_parcela'] = df_cons['data_primeira_parcela'].dt.strftime("%Y%m%d").astype('Int64')
df_cons['data_ultima_parcela'] = df_cons['data_ultima_parcela'].dt.strftime("%Y%m%d").astype('Int64')

# Obter id_cliente da dim_cliente baseado no nome_cliente
df_clientes = pd.read_sql('SELECT id_cliente, nome_cliente FROM dim_cliente', engine_silver)
df_cons = df_cons.merge(df_clientes, on='nome_cliente', how='left')

# Obter id_vendedor da dim_vendedor
df_vend = pd.read_sql('SELECT id_vendedor, nome_vendedor FROM dim_vendedor', engine_silver)
df_cons = df_cons.merge(df_vend, on='nome_vendedor', how='left')

# Selecionar e salvar colunas relevantes na tabela fato
colunas_fato = [
    "id_venda_consigcar", "id_cliente", "tipo_produto", "id_vendedor",
    "num_parcelas", "valor_parcela", "valor_total",
    "data_primeira_parcela", "data_ultima_parcela"
]
df_cons[colunas_fato].to_sql(
    "fato_vendas_consigcar", 
    engine_silver,
    if_exists="replace",
    index=False
)

# Verificar se há valores nulos após os merges
#null_counts = df_cons[["id_cliente", "id_vendedor"]].isnull().sum()
#print("Contagem de valores nulos após os merges:")
#print(null_counts)

# Se houver valores nulos, pode ser necessário investigar e tratar esses casos


# In[98]:


# 8) fato_vendas_diaria_vendedor
# Vem de: fato_vendas_consigcar

# Criar uma CTE para agregar vendas por vendedor e data
query = """
WITH vendas_por_dia AS (
    SELECT 
        v.id_vendedor,
        t.id_data,
        COUNT(*) as total_vendas,
        SUM(v.valor_total) as valor_total
    FROM fato_vendas_consigcar v
    JOIN dim_tempo t ON CAST(v.data_primeira_parcela AS VARCHAR) = t.id_data
    GROUP BY v.id_vendedor, t.id_data
)
SELECT *
FROM vendas_por_dia
"""

# Executar a query e carregar em um DataFrame
df_vendas_mensais = pd.read_sql(query, engine_silver)

# Salvar a tabela fato_vendas_diaria_vendedor no banco de dados
df_vendas_mensais.to_sql(
    "fato_vendas_diaria_vendedor",
    engine_silver,
    if_exists="replace", 
    index=False
)


# In[99]:


# 9) dim_pagamentos_programados
# Vem de: fato_vendas_consigcar

# Carregar os dados da tabela fato_vendas_consigcar
df_vendas = pd.read_sql_table('fato_vendas_consigcar', engine_silver)

# Função para gerar as datas de pagamento para cada venda
def gerar_datas_pagamento(row):
    data_inicial = pd.to_datetime(str(row['data_primeira_parcela']), format='%Y%m%d')
    datas = [data_inicial + pd.DateOffset(months=i) for i in range(row['num_parcelas'])]
    return pd.DataFrame({
        'id_venda_consigcar': row['id_venda_consigcar'],
        'valor': row['valor_parcela'],
        'id_data': [data.strftime('%Y%m%d') for data in datas]
    })

# Aplicar a função para cada venda e concatenar os resultados
df_pagamentos = pd.concat(df_vendas.apply(gerar_datas_pagamento, axis=1).tolist(), ignore_index=True)

# Ordenar o DataFrame
df_pagamentos = df_pagamentos.sort_values(['id_venda_consigcar', 'id_data'])

# Salvar o DataFrame na tabela dim_pagamentos_programados
df_pagamentos.to_sql('dim_pagamentos_programados', engine_silver, if_exists='replace', index=False)


# In[100]:


# 10) fato_metas_alucar
# Vem de: 00_base1.db."00_metas_plr"

# Carregar os dados da tabela de metas
df_metas_alu = pd.read_sql_table('00_metas_plr', engine_bronze1)

# Converter a coluna Data para id_data
df_metas_alu['id_data'] = convert_iddata(df_metas_alu, 'Data')

# Selecionar e renomear colunas
df_metas_alu = df_metas_alu[['id_data', 'Meta_1_ALUCAR', 'Meta_2_ALUCAR']]
df_metas_alu = df_metas_alu.rename(columns={
    'Meta_1_ALUCAR': 'meta_vendas_1_cum',
    'Meta_2_ALUCAR': 'meta_vendas_2_cum'
})

# Calcular metas mensais
df_metas_alu['meta_vendas_1_mes'] = df_metas_alu['meta_vendas_1_cum'].diff()
df_metas_alu['meta_vendas_2_mes'] = df_metas_alu['meta_vendas_2_cum'].diff()

# Para o primeiro mês, usar o valor cumulativo diretamente
first_month_mask = df_metas_alu['id_data'].astype(str).str[-4:-2] == '01'
df_metas_alu.loc[first_month_mask, 'meta_vendas_1_mes'] = df_metas_alu.loc[first_month_mask, 'meta_vendas_1_cum'].astype('int64')
df_metas_alu.loc[first_month_mask, 'meta_vendas_2_mes'] = df_metas_alu.loc[first_month_mask, 'meta_vendas_2_cum'].astype('int64')

# Salvar na tabela fato_metas_alucar
df_metas_alu.to_sql('fato_metas_alucar', engine_silver, if_exists='replace', index=False)


# In[101]:


# 11) fato_metas_consigcar
# Vem de: 00_base1.db."00_metas_plr"

# Carregar os dados da tabela de metas
df_metas_consig = pd.read_sql_table('00_metas_plr', engine_bronze1)

# Converter a coluna Data para id_data
df_metas_consig['id_data'] = convert_iddata(df_metas_consig, 'Data')

# Selecionar e renomear colunas
df_metas_consig = df_metas_consig[['id_data', 'Meta_1_ConsigCar', 'Meta_2_ConsigCar']]
df_metas_consig = df_metas_consig.rename(columns={
    'Meta_1_ConsigCar': 'meta_vendas_1_cum',
    'Meta_2_ConsigCar': 'meta_vendas_2_cum'
})

# Calcular metas mensais
df_metas_consig['meta_vendas_1_mes'] = df_metas_consig['meta_vendas_1_cum'].diff()
df_metas_consig['meta_vendas_2_mes'] = df_metas_consig['meta_vendas_2_cum'].diff()

# Para o primeiro mês, usar o valor cumulativo diretamente
first_month_mask = df_metas_consig['id_data'].astype(str).str[-4:-2] == '01'
df_metas_consig.loc[first_month_mask, 'meta_vendas_1_mes'] = df_metas_consig.loc[first_month_mask, 'meta_vendas_1_cum'].astype('int64')
df_metas_consig.loc[first_month_mask, 'meta_vendas_2_mes'] = df_metas_consig.loc[first_month_mask, 'meta_vendas_2_cum'].astype('int64')

# Salvar na tabela fato_metas_consigcar
df_metas_consig.to_sql('fato_metas_consigcar', engine_silver, if_exists='replace', index=False)


# In[102]:


# 12) fato_vendas_clientes_alucar_estimativa
# Vem de: 00_base1.db."00_vendas_clientes_alucar_estimativa"
# Carregar dados da tabela de estimativa de vendas
df_vendas_estimativa = pd.read_sql_table('00_vendas_clientes_alucar_estimativa', engine_bronze1)

# Converter a coluna Data para id_data
df_vendas_estimativa['id_data'] = convert_iddata(df_vendas_estimativa, 'Data')

# Selecionar e renomear colunas
df_vendas_estimativa = df_vendas_estimativa[['id_data', 'Valor_Receita']]
df_vendas_estimativa = df_vendas_estimativa.rename(columns={'Valor_Receita': 'valor_receita_estimativa'})

# Remover 'R$' e converter para valor numérico
df_vendas_estimativa['valor_receita_estimativa'] = (
    df_vendas_estimativa['valor_receita_estimativa']
    .str.replace('R$', '')
    .str.replace('.', '')
    .str.replace(',', '.')
    .astype(float)
)

# Salvar na tabela fato_vendas_clientes_alucar_estimativa
df_vendas_estimativa.to_sql('fato_vendas_clientes_alucar_estimativa', engine_silver, if_exists='replace', index=False)


# In[103]:


# 13) fato_consigcar_estimativa
# Carregar dados da tabela de estimativa de receita consigcar
df_consig_estimativa = pd.read_sql_table('00_receita_consigcar_estimativa', engine_bronze1)

# Converter a coluna Data para id_data
df_consig_estimativa['id_data'] = pd.to_datetime(df_consig_estimativa['Data']).dt.strftime('%Y%m%d').astype('Int64')

# Selecionar e renomear colunas
df_consig_estimativa = df_consig_estimativa[['id_data', 'Valor']]
df_consig_estimativa = df_consig_estimativa.rename(columns={'Valor': 'valor_receita_estimativa'})

# Remover 'R$' e converter para valor numérico
df_consig_estimativa['valor_receita_estimativa'] = (
    df_consig_estimativa['valor_receita_estimativa']
    .str.replace('R$', '')
    .str.replace('.', '')
    .str.replace(',', '.')
    .astype(float)
)

# Salvar na tabela fato_consigcar_estimativa
df_consig_estimativa.to_sql('fato_consigcar_estimativa', engine_silver, if_exists='replace', index=False)

print(df_consig_estimativa)


# In[104]:


# Transformar o banco de dados em DBML para acompanhamento da modelagem

dbml = sql_to_dbml(engine_silver)
with open("../modelagem/01_silver.dbml", "w", encoding="utf-8") as f:
    f.write(dbml)

