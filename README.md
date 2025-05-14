# Hackathon2025
Repositório do projeto proposto no Hackathon 2025 da EmpregaDados
=============================================================
## 🚀 Objetivo
Construir um pipeline ETL que:
- Extraia dados brutos de diferentes fontes (CSV);
- Faça o tratamento e padronização das colunas (camada *Silver*);
- Entregue os dados prontos para visualização e análises (*Gold*);
- Geração de dashboards para tomada de decisões;
- Garanta rastreabilidade, escalabilidade e reprodutibilidade.

## 🧪 Como Rodar

1. Clone o repositório:
```bash
git clone https://github.com/AlvaroBernardino/hackathon2025
cd hackathon2025
```
2. Instale as dependências:
```bash
pip install -r requirements.txt
```
3. Baixe o CSV fonte:
- Acesse [a tabela base 2](https://empregadados-my.sharepoint.com/:x:/g/personal/bianca_empregadados_com_br/EZYutqfo5ldNhDw2lMYRxrIBnpPI6c7OTjBBS_F5yz860Q?rtime=to82V2qS3Ug)
- Exporte como CSV na pasta "database"
  
4. Execute os notebooks
- Execute os notebooks da pasta "etl" na ordem numérica

## 🧪 Diretórios
 - *config*: Arquivos de configuração
 - *data*: Dados crus (Landing zone)
 - *database*: Arquivos de bancos de dados
 - *etl*: Scripts de ETL
 - *mkdown*: Arquivos de texto que podem ser úteis
 - *modelagem*: Arquivos .dbml para visualização fácil dos schemas
 - *retired*: Scripts e snippets de legado, que não serão utilizados no pipeline
 - *changelog.md*: Histórico de modificações. Nas mensagens dos commits, colocar simplesmente o nome da versão (Ex.: 0.2.0) e colocar as mudanças neste arquivo.

## ✍️ Autores
- [Álvaro Bernardino](https://www.linkedin.com/in/alvaro-bernardino/)
- [Caio Prates](https://www.linkedin.com/feed/)
- [Diego Simon](https://www.linkedin.com/in/diego-simon/)
- [Laerte Rocha Neves](https://www.linkedin.com/in/laerterochanp/)
- [Luiz Henrique Popoff](https://www.linkedin.com/in/luizpopoff/)
