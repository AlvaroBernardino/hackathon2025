from sqlalchemy import inspect
import pandas as pd

## Função para editar a tabela de Receita
def edit_sheet(df):
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]  
    df.columns = df.columns.str.replace('\n', '_', regex=True)
    df.columns = df.columns.str.replace(' ', '_', regex=True)
    return df

## Função para converter o SQL para DBML
def sql_to_dbml(engine):
    inspector = inspect(engine)
    dbml = ""

    # Mapeia referências por tabela e coluna
    foreign_keys_map = {}
    for table_name in inspector.get_table_names():
        for fk in inspector.get_foreign_keys(table_name):
            if not fk.get('referred_table'):
                continue
            for col, ref_col in zip(fk['constrained_columns'], fk['referred_columns']):
                foreign_keys_map[(table_name, col)] = f"ref: > {fk['referred_table']}.{ref_col}"

    # Monta o DBML
    for table_name in inspector.get_table_names():
        dbml += f"Table {table_name} {{\n"
        columns = inspector.get_columns(table_name)
        pk_constraint = inspector.get_pk_constraint(table_name)
        pk_columns = pk_constraint.get('constrained_columns') if pk_constraint else []
        if pk_columns is None:
            pk_columns = []

        for column in columns:
            name = column['name']
            dtype = str(column['type'])  # Converter para string legível
            nullable = column.get('nullable', True)
            is_primary = name in pk_columns

            options = []
            if is_primary:
                options.append("pk")
                # autoincrement pode não estar presente, então checar com get e garantir bool
                if column.get('autoincrement', False) or column.get('autoincrement') == 'auto':
                    options.append("increment")
            elif not nullable:
                options.append("not null")

            if (table_name, name) in foreign_keys_map:
                options.append(foreign_keys_map[(table_name, name)])

            options_str = f" [{', '.join(options)}]" if options else ""
            dbml += f"  {name} {dtype}{options_str}\n"

        dbml += "}\n\n"

    return dbml

## Função para converter a coluna de data para o formato id_data
def convert_iddata(df, col_name):
    return pd.to_datetime(df[col_name], format="%Y%m%d").dt.strftime("%Y%m%d").astype("Int64")

## Função para classificar as despesas
def classificar_despesa(item):
    if item in [
        'Despesas com matéria-prima',
        'Despesas de transporte e logística',
        'Mecanicos',
        'Manutenção de equipamentos'
    ]:
        return 'Custos Variáveis (CPV)'

    elif item in [
        'Despesas com marketing e publicidade',
        'Despesas com viagens e deslocamentos',
        'Despesas com telefonia móvel e fixa',
        'Trafego', 'Google Ads', 'Meta Ads', 'LinkedIn Ads', 'CRM', 'Landin page'
    ]:
        return 'Despesas Comerciais'

    elif item in [
        'Impostos e taxas'
    ]:
        return 'Impostos'

    elif item in [
        'Treinamento'
    ]:
        return 'Desenvolvimento Pessoal'

    else:
        return 'Despesas Administrativas'