# Stock Market ETL Pipeline

Pipeline de dados do mercado de ações brasileiro (B3) usando Apache Airflow, Python e PostgreSQL.

## Arquitetura

```
Yahoo Finance API  →  Extract (yfinance)  →  Transform (pandas)  →  Load (PostgreSQL)
                          ↓                       ↓                       ↓
                    raw JSON files          cleaned CSVs           Star Schema DW
```

### Star Schema

```
┌──────────────┐     ┌───────────────────┐     ┌──────────────┐
│  dim_company │────→│ fact_stock_price   │←────│   dim_date   │
│              │     │                   │     │              │
│ ticker       │     │ open/high/low/    │     │ full_date    │
│ name         │     │ close/volume      │     │ year/month   │
│ sector       │     │ daily_return      │     │ day_of_week  │
│ industry     │     │                   │     │ is_weekend   │
└──────────────┘     └───────────────────┘     └──────────────┘
                     ┌───────────────────┐
                     │ fact_daily_summary │
                     │                   │
                     │ avg_return         │
                     │ best/worst ticker  │
                     │ total_volume       │
                     └───────────────────┘
```

### Ações Monitoradas

| Ticker    | Empresa          |
|-----------|------------------|
| PETR4.SA  | Petrobras        |
| VALE3.SA  | Vale             |
| ITUB4.SA  | Itaú Unibanco    |
| BBDC4.SA  | Bradesco         |
| ABEV3.SA  | Ambev            |
| WEGE3.SA  | WEG              |
| RENT3.SA  | Localiza         |
| BBAS3.SA  | Banco do Brasil  |
| SUZB3.SA  | Suzano           |
| GGBR4.SA  | Gerdau           |

## Tech Stack

- **Orquestração**: Apache Airflow 2.8
- **Extração**: yfinance (Yahoo Finance API)
- **Transformação**: pandas
- **Data Warehouse**: PostgreSQL 15 (Star Schema)
- **Infraestrutura**: Docker / Docker Compose

## Como Executar

### Pré-requisitos

- Docker e Docker Compose instalados

### 1. Clone o repositório

```bash
git clone https://github.com/willrosa93/stock-market-etl.git
cd stock-market-etl
```

### 2. Suba os containers

```bash
docker-compose up -d
```

### 3. Acesse o Airflow

- URL: http://localhost:8081
- Usuário: `admin`
- Senha: `admin`

### 4. Ative a DAG

No painel do Airflow, ative a DAG `stock_market_etl` e execute manualmente ou aguarde o schedule (dias úteis às 20h).

### 5. Consulte os dados

Conecte no warehouse PostgreSQL:

```bash
psql -h localhost -p 5434 -U warehouse -d stock_warehouse
```

Exemplo de query:

```sql
SELECT
    c.ticker,
    c.name,
    d.full_date,
    p.close_price,
    p.daily_return,
    p.volume
FROM fact_stock_price p
JOIN dim_company c ON c.company_id = p.company_id
JOIN dim_date d ON d.date_id = p.date_id
ORDER BY d.full_date DESC, c.ticker
LIMIT 20;
```

## Estrutura do Projeto

```
.
├── dags/
│   └── stock_etl_dag.py       # DAG definition
├── scripts/
│   ├── extract.py              # Yahoo Finance extraction
│   ├── transform.py            # Data transformation (pandas)
│   └── load.py                 # PostgreSQL loading
├── sql/
│   └── create_tables.sql       # Star schema DDL
├── data/                       # Generated data files (gitignored)
├── logs/                       # Airflow logs (gitignored)
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Pipeline (DAG)

```
extract_stock_data → transform_stock_data → load_dim_company → load_fact_stock_price → load_fact_daily_summary
```

- **Extract**: Baixa dados dos últimos 5 dias úteis via yfinance
- **Transform**: Limpa dados, calcula retorno diário, gera resumo agregado
- **Load (companies)**: Upsert na dimensão de empresas
- **Load (prices)**: Upsert nos fatos de preços
- **Load (summary)**: Upsert no resumo diário
