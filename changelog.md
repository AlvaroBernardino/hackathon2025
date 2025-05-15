# Changelog

Todas as mudanças importantes neste projeto serão documentadas aqui.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/)
e este projeto segue a [SemVer](https://semver.org/lang/pt-BR/).

## [0.3.0] - 2025-05-15
### Adicionado
- Funções movidas para `__init__.py`
- Função de leitura de planilha direto de link do SharePoint (bronze)
- Script de transformação para camada silver
- Script de testes para os bancos de dados

## [0.2.0] - 2025-05-14

### Adicionado
- Função `sql_to_dbml` para gerar modelagem em DBML a partir do SQLite.
- Leitura de planilhas direto do Google Sheets.

### Corrigido
- Problema de encoding na base 2.

## [0.1.0] - 2025-05-14

### Adicionado
- Início do projeto.
- Extração e tratamento de dados da planilha de receita.
