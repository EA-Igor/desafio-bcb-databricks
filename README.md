# Desafio BCB - SELIC, IPCA e juro real

Pipeline Databricks para montar uma base analítica com duas séries públicas do
SGS do Banco Central do Brasil:

- SELIC diária: série 11
- IPCA mensal: série 433

O período usado no desafio é de `01/01/2020` a `31/12/2024`.

## Arquitetura

O projeto usa três camadas no Unity Catalog:

- Bronze: ingestão idempotente dos arquivos brutos `selic.json` e `ipca.json`
  a partir do Volume `/Volumes/desafio_bcb/default/raw_files`.
- Silver: padronização de datas, tipagem de valores numéricos e carga
  idempotente por `MERGE`.
- Gold: tabela mensal consolidada com SELIC média do mês, IPCA do mês, juro
  real mensal e juro real acumulado em 12 meses.

As tabelas são criadas no catálogo/schema:

```text
desafio_bcb.default
```

## Estrutura

```text
extract/
  extract_bcb.py
scripts/
  upload_to_volume.py
src/desafio_bcb/
  bronze.py
  silver.py
  gold.py
  quality.py
  idempotency.py
notebooks/
  01_bronze.py
  02_silver.py
  03_gold.py
  04_idempotency_report.py
databricks/
  workflow.json
resources/
  desafio_bcb_job.yml
databricks.yml
sql/
  idempotency_checks.sql
```

## Etapa 0 - Extração local

O Databricks Free Edition pode bloquear chamadas HTTP diretas para o Banco
Central. Por isso, a extração roda localmente:

```powershell
python extract/extract_bcb.py --output-dir data/raw
```

O script baixa os arquivos:

```text
data/raw/selic.json
data/raw/ipca.json
```

Ele trata erro de rede, aplica retentativas com backoff exponencial e falha de
forma explícita se a API não responder ou devolver payload vazio/malformado.

## Upload para o Volume

No Databricks:

1. Acesse `Catalog`.
2. Use o catálogo `desafio_bcb` e o schema `default`.
3. Crie ou selecione o Volume `raw_files`.
4. Faça upload dos arquivos:
   - `data/raw/selic.json`
   - `data/raw/ipca.json`

O caminho esperado pelo pipeline é:

```text
/Volumes/desafio_bcb/default/raw_files
```

### Automacao via Databricks CLI

Como alternativa ao upload manual pela UI, o projeto inclui um script que usa a
Databricks CLI autenticada para enviar os arquivos ao Volume:

```powershell
python scripts/upload_to_volume.py
```

O script valida que `selic.json` e `ipca.json` existem localmente, que os JSONs
nao estao vazios, cria o diretorio de destino no Volume e executa o upload com
sobrescrita habilitada.

Pre-requisito:

```powershell
databricks auth login --host https://<seu-workspace>
```

Parametros uteis:

```powershell
python scripts/upload_to_volume.py --dry-run
python scripts/upload_to_volume.py --profile <profile-name>
python scripts/upload_to_volume.py --volume-path /Volumes/desafio_bcb/default/raw_files
python scripts/upload_to_volume.py --cli-path "C:\caminho\para\databricks.exe"
```

Internamente, o script converte o caminho `/Volumes/...` para o formato exigido
pela CLI, `dbfs:/Volumes/...`.

## Pipeline

O workflow está definido em:

```text
databricks/workflow.json
```

Ele executa as tarefas nesta ordem:

1. `bronze`
2. `silver`
3. `gold`
4. `idempotency_report`

Os notebooks em `notebooks/` são apenas entrypoints. A lógica de negócio fica
nos módulos em `src/desafio_bcb/`.

## Deploy com Databricks Asset Bundle

O projeto também possui um Databricks Asset Bundle para deploy do Workflow como
código:

```text
databricks.yml
resources/desafio_bcb_job.yml
```

O Bundle cria o job `desafio-bcb-bronze-silver-gold` com as quatro tasks do
pipeline em compute serverless. Antes de executar, configure a Databricks CLI no
workspace desejado:

```powershell
databricks auth login --host https://<seu-workspace>
```

Valide, faça o deploy e execute o job:

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev desafio_bcb_pipeline
```

Os parâmetros padrão do Bundle são:

```text
catalog = desafio_bcb
schema = default
raw_volume_path = /Volumes/desafio_bcb/default/raw_files
```

Para executar um backfill em outro subdiretório do Volume, informe outro
`raw_volume_path` na execução do job ou ajuste a variável correspondente em
`databricks.yml`.

## Chaves de negócio e idempotência

Bronze:

- `bronze_selic_raw`: `series_name`, `data`, `source_file`
- `bronze_ipca_raw`: `series_name`, `data`, `source_file`

Silver:

- `silver_selic`: `series_name`, `reference_date`
- `silver_ipca`: `series_name`, `reference_date`

Gold:

- `gold_monthly_real_interest`: `reference_month`

Todas as cargas usam `MERGE`. Rodar o workflow duas vezes não duplica registros.

## Grão das tabelas

Bronze:

- Um registro por observação bruta da série no arquivo de origem.
- Datas e valores são preservados como `STRING`.

Silver:

- SELIC: um registro por dia da série SELIC.
- IPCA: um registro por mês da série IPCA.
- Datas são convertidas para `DATE` e valores para `DECIMAL(18,8)`.

Gold:

- Um registro por mês.
- A tabela contém apenas meses que possuem SELIC e IPCA.

## Métricas da Gold

Tabela:

```text
desafio_bcb.default.gold_monthly_real_interest
```

Colunas:

- `reference_month`: primeiro dia do mês.
- `selic_avg_month_pct`: média mensal da SELIC diária.
- `ipca_month_pct`: IPCA do mês.
- `real_interest_month_pct`: juro real mensal pela fórmula de Fisher:
  `((1 + selic / 100) / (1 + ipca / 100) - 1) * 100`.
- `real_interest_accumulated_12m_pct`: acumulação móvel de 12 meses do juro
  real mensal.

## Qualidade de dados

O job falha explicitamente se qualquer regra abaixo for violada:

- Dataset de origem vazio.
- Campos obrigatórios nulos após leitura ou tipagem.
- Chaves de negócio duplicadas em Bronze, Silver ou Gold.
- Gold com quantidade diferente de 60 meses para o período 2020-2024.

## Evidência de idempotência

Execute o workflow duas vezes no Databricks. Depois rode o notebook:

```text
notebooks/04_idempotency_report.py
```

ou a consulta:

```text
sql/idempotency_checks.sql
```

O resultado esperado é `duplicate_key_count = 0` para todas as tabelas.

## Estratégia de backfill

O pipeline foi desenhado para permitir reprocessamentos históricos sem duplicar
registros. As camadas Bronze, Silver e Gold usam chaves de negócio estáveis e
cargas idempotentes por `MERGE`, então o mesmo intervalo pode ser carregado mais
de uma vez com o mesmo resultado final.

Para executar um backfill, o fluxo recomendado é:

1. Gerar novamente os arquivos brutos pelo script local, ajustando o período das
   URLs da API do BCB quando for necessário reprocessar outro intervalo.
2. Fazer upload dos arquivos para o Volume do Unity Catalog.
3. Executar o Workflow completo no Databricks.
4. Validar o resultado com o relatório de idempotência.

Para backfills pontuais dentro do período atual, os arquivos podem substituir os
arquivos existentes no Volume:

```text
/Volumes/desafio_bcb/default/raw_files/selic.json
/Volumes/desafio_bcb/default/raw_files/ipca.json
```

Para backfills maiores ou recorrentes, a recomendação é versionar os arquivos
brutos por intervalo ou data de extração, por exemplo:

```text
/Volumes/desafio_bcb/default/raw_files/backfill_2020_2024/selic.json
/Volumes/desafio_bcb/default/raw_files/backfill_2020_2024/ipca.json
```

Nesse cenário, o parâmetro `raw_volume_path` do Workflow pode apontar para o
subdiretório do backfill. A Bronze preserva o `source_file`, mantendo
rastreabilidade sobre qual arquivo originou cada registro. A Silver usa
`series_name` e `reference_date` como chave de negócio, e a Gold usa
`reference_month`, evitando duplicações mesmo em reexecuções.
