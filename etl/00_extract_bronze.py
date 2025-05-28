#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Bibliotecas
import pandas as pd
import os
from sqlalchemy import create_engine, inspect, text
from io import BytesIO
import requests
from datetime import datetime
from utils import sql_to_dbml, edit_sheet # Importando as funções do arquivo utils.py


# In[2]:


# Dicionários
# com os caminhos do arquivos e atributos da tabela

base1 = {
    "sheet_id": "1cucnW4yVosO5n5BFgwXYv6rVy8yj6NTasM83RTCMOug",
    "gid_receita": "373473243",
    "gid_despesas": "1859279676",
    "encoding": "latin1",
    "db_path": os.path.abspath("../database/bronze/00_base1.db"),
    "dbml_path": os.path.abspath("../modelagem/00_base1.dbml"),
}

base2 = {
    "url": "https://empregadados-my.sharepoint.com/personal/bianca_empregadados_com_br/_layouts/15/download.aspx?share=EZYutqfo5ldNhDw2lMYRxrIBnpPI6c7OTjBBS_F5yz860Q",
    "db_path": os.path.abspath("../database/bronze/00_base2.db"),
    "dbml_path": os.path.abspath("../modelagem/00_base2.dbml"),
    "encoding": "latin1",
}


# ## Extraindo Base 1 (Vendas)

# ### Aba Receita

# In[3]:


#Lendo e armazenando em um dataframe as tabelas
df_Receita = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{base1['sheet_id']}/export?format=csv&gid={base1['gid_receita']}", index_col=False)
df_Receita = edit_sheet(df_Receita)


# In[4]:


# Inciando tratamento
# Exclusão de linhas sem dados nas colunas Data e Mês
df_Receita = df_Receita.dropna(subset=["Data", "Mes"])

# Transformação da coluna data
df_Receita['Data'] = pd.to_datetime(df_Receita['Data'], dayfirst = True).dt.date

# Transformação das colunas Mês e Ano para inteiro
df_Receita["Mes"] = df_Receita["Mes"].astype(int)
df_Receita["Ano"] = df_Receita["Ano"].astype(int)

# Separação das colunas da Alucar e Consigcar
df_Receita_Alucar = df_Receita.iloc[:, 0:5] \
                    .reset_index(drop = True)

df_Receita_ConsigCar = df_Receita.iloc[:, [1,6]] \
                        .dropna(subset=["Valor"]) \
                        .reset_index(drop = True)

############# Verificar isso depois #############
# Gambiarra para corrigir o problema com a primeira data da tabela
df_Receita_ConsigCar.loc[0, 'Data'] = pd.to_datetime('2025-01-01').date()


# Separação do dataframe da Receita da Alucar e estimativa da Alucar
df_Receita_Alucar_estimativa = df_Receita_Alucar[df_Receita_Alucar["Nome_(Alucar)"] == 'Estimativa'] \
                                .reset_index(drop = True)

df_Receita_Alucar = df_Receita_Alucar[df_Receita_Alucar["Nome_(Alucar)"] != 'Estimativa']

# Cria conexão com banco de dados SQLite
engine1 = create_engine(f"sqlite:///{base1['db_path']}")

# Insere as tabelas no banco
df_Receita_Alucar.to_sql("00_vendas_clientes_alucar", con=engine1, if_exists="replace", index=False)
df_Receita_Alucar_estimativa.to_sql("00_vendas_clientes_alucar_estimativa", con=engine1, if_exists="replace", index=False)
df_Receita_ConsigCar.to_sql("00_receita_pagseguro_consigcar", con=engine1, if_exists="replace", index=False)


# ### Aba Despesas 

# In[5]:


# Lendo e armazenando em um dataframe as tabelas
df_Despesas = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{base1["sheet_id"]}/export?format=csv&gid={base1["gid_despesas"]}", index_col=False)

# Excluindo colunas desnecessárias
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

# Cria conexão com banco de dados SQLite
engine1 = create_engine(f"sqlite:///{base1["db_path"]}")

# Insere as tabelas no banco
df_Despesas_Alucar.to_sql("00_despesas_alucar", con=engine1, if_exists="replace", index=False)
df_Despesas_ConsigCar.to_sql("00_despesas_consigcar", con=engine1, if_exists="replace", index=False)


# ### Extraindo Base 2

# In[6]:


# Lendo e aramazenando em um dataframe as tabelas

response = requests.get(base2["url"])
df = pd.read_excel(BytesIO(response.content))

# Remove 'R$', pontos e converte a vírgula decimal para ponto
df['Valor parcela'] = df['Valor parcela'].replace({'R\$': '', '\.': '', ',': '.'}, regex=True).astype(float)

# Converte a coluna de data e remove a hora
df['Data do Pagamento'] = pd.to_datetime(df['Data do Pagamento']).dt.date

# Cria conexão com banco de dados SQLite
engine2 = create_engine(f"sqlite:///{base2['db_path']}")

# Insere a tabela no banco
df.to_sql("00_vendas_clientes_consigcar", con=engine2, if_exists="replace", index=False)


# In[10]:


# Timestamp
# Configurações das engines (base1 e base2)
engine_base1 = create_engine(f"sqlite:///{base1['db_path']}")
engine_base2 = create_engine(f"sqlite:///{base2['db_path']}")

# Obter tabelas
with engine_base1.connect() as conn:
    tables_base1 = [table[0] for table in conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))]

with engine_base2.connect() as conn:
    tables_base2 = [table[0] for table in conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))]

now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Processar base1
with engine_base1.connect() as conn:
    for table in tables_base1:
        # Verificar se a coluna 'timestamp' existe
        columns = [col[0] for col in conn.execute(text(f'PRAGMA table_info("{table}")'))]
        if 'timestamp' not in columns:
            conn.execute(text(f'ALTER TABLE "{table}" ADD COLUMN timestamp TEXT DEFAULT "{now}"'))
        else:
            conn.execute(text(f'UPDATE "{table}" SET timestamp = :now WHERE timestamp IS NULL'), {'now': now})
        conn.commit()  # Persistir as alterações

# Processar base2 (mesma lógica)
with engine_base2.connect() as conn:
    for table in tables_base2:
        columns = [col[0] for col in conn.execute(text(f'PRAGMA table_info("{table}")'))]
        if 'timestamp' not in columns:
            conn.execute(text(f'ALTER TABLE "{table}" ADD COLUMN timestamp TEXT DEFAULT "{now}"'))
        else:
            conn.execute(text(f'UPDATE "{table}" SET timestamp = :now WHERE timestamp IS NULL'), {'now': now})
        conn.commit()


# In[9]:


now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"Valor de 'now': {now}")  # Debug


# In[8]:


# Transformar os bancos de dados em DBML para acompanhamento da modelagem
dbml = sql_to_dbml(engine2)

with open(base2["dbml_path"], "w", encoding="utf-8") as f:
    f.write(dbml)

dbml = sql_to_dbml(engine1)

with open(base1["dbml_path"], "w", encoding="utf-8") as f:
    f.write(dbml)

