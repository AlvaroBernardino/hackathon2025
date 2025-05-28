#!/usr/bin/env python
# coding: utf-8

# In[7]:


from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = f"postgresql+psycopg2://{os.getenv('user')}:{os.getenv('password')}@{os.getenv('host')}:{os.getenv('port')}/{os.getenv('dbname')}?sslmode=require"

engine = create_engine(DATABASE_URL)

try:
    with engine.connect() as conn:
        print("Conectado com sucesso!")
except Exception as e:
    print("Erro ao conectar:", e)

