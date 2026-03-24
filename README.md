# Painel de Evolução Territorial do Bolsa Família

Projeto em `Python + PySpark + Streamlit` para analisar a evolução territorial do Bolsa Família com base em dados públicos abertos e complementar a leitura com uma camada operacional de `pagamento vs saque`.

Por padrão, a execução local usa um fallback em `pandas` para garantir reprodutibilidade mesmo em ambientes sem `Java`. O pipeline `PySpark` continua implementado no projeto e pode ser ativado definindo `USE_PYSPARK=1` em um ambiente com Java configurado.

## O que é o Bolsa Família

O Bolsa Família é o principal programa de transferência de renda do Brasil. Segundo o Ministério do Desenvolvimento e Assistência Social, Família e Combate à Fome, o programa busca garantir renda para famílias em situação de pobreza, fortalecer o acesso a direitos básicos e articular proteção social com outras políticas públicas. Para ter direito ao programa, a regra principal considera a renda mensal por pessoa da família, e a entrada depende de inscrição e atualização no Cadastro Único. O desenho atual do benefício considera o tamanho e a composição da família, com parcelas e complementos específicos para diferentes perfis familiares.  
Fontes: [MDS - Bolsa Família](https://www.gov.br/mds/pt-br/acoes-e-programas/bolsa-familia), [MDS - Benefícios do Bolsa Família](https://www.gov.br/mds/pt-br/acesso-a-informacao/perguntas_frequentes/bolsa-familia-beneficiario/2-quais-sao-os-beneficios)

## Para que serve

Este projeto serve para reproduzir um tipo de análise comum em políticas públicas e gestão social:

- acompanhar a evolução do valor repassado ao longo do tempo;
- comparar municípios em termos de famílias beneficiárias e benefício médio;
- identificar variações territoriais relevantes;
- simular uma visão operacional de `pagamento disponibilizado` vs `saque estimado`, destacando possíveis gaps que mereceriam monitoramento.
- prever repasses futuros com um modelo supervisionado;
- segmentar municípios por perfil socioeconômico e operacional;
- detectar anomalias em comportamento de pagamento vs saque.

Na prática, ele simula um fluxo que faria sentido em times de dados do setor público:

- acompanhar a expansão ou retração do programa no território;
- comparar municípios de portes e perfis diferentes;
- identificar locais com maior concentração de famílias atendidas;
- monitorar sinais operacionais que poderiam justificar investigação adicional;
- apoiar priorização de municípios para análise, gestão e auditoria.

## Nome mais adequado para o repositório

Se você quiser renomear o projeto depois no GitHub, os nomes que fazem mais sentido agora são:

- `bolsa-familia-territorial-analytics`
- `bolsa-familia-social-analytics`
- `bolsa-familia-monitoring-and-ml`

Minha recomendação principal:

- `bolsa-familia-territorial-analytics`

## Fonte de dados

Base pública utilizada:

- `Bolsa Família` por município de `Alagoas`, disponível no portal de dados abertos do governo de Alagoas.

Arquivo usado:

- [al_bolsa_familia_municipios.csv](data/raw/al_bolsa_familia_municipios.csv)

Cobertura:

- `2004` a `2023`, com ausência de `2022` na base pública disponível
- `102` municípios
- indicadores anuais de:
  - valor total repassado
  - famílias beneficiárias
  - benefício médio

## O que significam os dados

O dataset público usado aqui é uma base agregada por município e ano. Isso significa que ele não traz registros individuais de famílias ou beneficiários, mas sim indicadores consolidados por território.

As colunas mais importantes para a análise são:

- `co_mun` / `codigo_municipio`
  Identificador do município.
- `no_mun` / `municipio`
  Nome do município.
- `ano`
  Ano de referência da observação.
- `social_subcategoria`
  Tipo de indicador observado no ano:
  - `Valor Total Repassado do Bolsa Família`
  - `Famílias beneficiárias`
  - `Benefício médio recebido pelas famílias do Bolsa Família`
- `valor`
  Valor numérico associado ao indicador.

A partir desses campos, o projeto reconstrói três dimensões analíticas principais:

1. `Evolução territorial`
   Como o volume do programa muda no tempo e no espaço.
2. `Perfil municipal`
   Como municípios se diferenciam em escala, benefício médio e cobertura.
3. `Operação simulada`
   Como seria um monitoramento mensal de pagamento disponibilizado vs saque estimado.

### Nota metodológica importante

A parte de `evolução territorial` usa base pública real.

A parte de `pagamento vs saque` é uma **camada operacional sintética**, calibrada a partir dos valores públicos anuais. Ela foi criada porque os downloads transacionais oficiais de pagamento e saque do Portal da Transparência estavam indisponíveis para consumo automatizado no ambiente desta execução. A modelagem sintética preserva o caso de uso analítico, mas não deve ser interpretada como dado oficial transacional.

## Técnicas e ferramentas usadas

- `PySpark`
  Para leitura, transformação e agregação em estilo big data.
- `Spark SQL functions`
  Para pivot, janelas analíticas e cálculo de crescimento percentual.
- `Pandas`
  Para consumo leve no dashboard.
- `scikit-learn`
  Para regressão, clustering e detecção de anomalias.
- `Streamlit`
  Para o painel interativo.
- `Plotly`
  Para gráficos territoriais e operacionais.

## O que são as técnicas usadas

### Engenharia de dados e agregação

O projeto primeiro organiza a base em formato analítico:

- leitura do CSV bruto;
- padronização de nomes e tipos;
- `pivot` das linhas por subcategoria para transformar indicadores em colunas;
- cálculo de crescimento anual;
- criação de uma camada mensal sintética para monitoramento operacional.

Essa etapa é importante porque dados públicos administrativos costumam vir em formato tabular “longo”, mas análises territoriais e modelos precisam de uma estrutura mais estável por município e por período.

### Regressão para prever repasse futuro

A camada de regressão tenta prever o `valor_total_repassado` no ano mais recente usando sinais históricos do próprio município.

Tecnicamente, isso funciona como um problema supervisionado de regressão:

- entrada: lags do repasse, número de famílias, benefício médio e crescimento anterior;
- saída: valor total repassado no período seguinte.

Modelo usado:

- `RandomForestRegressor`

Por que faz sentido aqui:

- lida bem com relações não lineares;
- não exige muitos pressupostos estatísticos fortes;
- funciona bem como baseline para dados tabulares públicos.

Métrica usada:

- `R²`
  Mede quanto da variabilidade do valor repassado o modelo consegue explicar.

### Clustering de municípios

O clustering agrupa municípios com perfis parecidos de repasse, cobertura e comportamento operacional.

Modelo usado:

- `KMeans`

Objetivo:

- encontrar perfis municipais semelhantes sem precisar de rótulos prévios.

Métrica usada:

- `silhouette score`
  Mede o quão bem separados e coesos os grupos estão.

### Detecção de anomalias operacionais

Essa camada usa a base operacional mensal sintética para identificar pontos fora do padrão.

Modelo usado:

- `IsolationForest`

Objetivo:

- encontrar municípios e meses com combinações atípicas de pagamento, saque, gap e taxa de saque.

Esse tipo de abordagem é útil quando queremos priorizar inspeção humana, auditoria ou revisão gerencial sem depender de um rótulo histórico explícito de erro.

## Pipeline

1. Leitura da base pública em CSV.
2. Padronização do schema.
3. Pivot das subcategorias em métricas anuais por município.
4. Cálculo de crescimento anual de repasses e famílias.
5. Geração da camada operacional mensal sintética para `2021-2023`.
6. Cálculo de:
   - valor pago estimado
   - valor sacado estimado
   - gap entre pagamento e saque
   - taxa de saque
   - risco operacional
7. Escrita em `parquet`.
8. Visualização no dashboard.

## Camadas de machine learning adicionadas

### 1. Regressão para prever repasse futuro

Objetivo:

- prever o `valor_total_repassado` do ano mais recente disponível a partir de sinais históricos do próprio município.

Modelo usado:

- `RandomForestRegressor`

Features:

- `ano`
- `lag_valor_1`
- `lag_valor_2`
- `lag_familias_1`
- `lag_beneficio_1`
- `lag_crescimento_1`

Leitura:

- como a base é anual e há ausência de `2022`, a tarefa de previsão é difícil e o `R²` deve ser lido como um benchmark exploratório, não como modelo final de produção.

### 2. Clustering de municípios

Objetivo:

- agrupar municípios com perfis parecidos de repasse, benefício, famílias atendidas e comportamento operacional.

Modelo usado:

- `KMeans`

Critério:

- busca do melhor `k` entre `3` e `6` usando `silhouette score`

Features:

- `valor_total_repassado`
- `familias_beneficiarias`
- `beneficio_medio`
- `crescimento_valor_pct`
- `taxa_saque_media`
- `gap_medio`

### 3. Detecção de anomalias operacionais

Objetivo:

- identificar meses e municípios com comportamento atípico em `pagamento vs saque`.

Modelo usado:

- `IsolationForest`

Features:

- `valor_pago_estimado`
- `valor_sacado_estimado`
- `gap_pagamento_saque`
- `taxa_saque_pct`
- `familias_mes_estimadas`

## Resultados atuais

- `102` municípios cobertos
- `19` anos de histórico
- `2.448` linhas operacionais sintéticas
- taxa média estimada de saque acima de `90%`
- `R² da regressão`: `0.1515`
- `clusters selecionados`: `3`
- `silhouette score`: `0.3158`
- `anomalias operacionais`: `74`

## Como interpretar os resultados

- A parte territorial é a mais forte do projeto, porque está baseada diretamente em dados públicos reais.
- A regressão deve ser lida como uma linha de base exploratória, não como modelo final de previsão fiscal, já que a série é anual, curta e tem lacuna em `2022`.
- O clustering é útil para separar perfis de municípios e apoiar segmentação territorial.
- A detecção de anomalias é útil para priorizar investigação operacional, especialmente quando não existe uma base rotulada de problemas.

## Como executar

```bash
cd "/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/evolucao-territorial-do-bolsa-familia"
source .venv/bin/activate
python main.py
streamlit run app.py
```

Para forçar o backend PySpark em um ambiente com Java:

```bash
USE_PYSPARK=1 python main.py
```

## Testes

```bash
source .venv/bin/activate
python -m unittest discover -s tests -v
```

## English

### What Bolsa Família is

Bolsa Família is Brazil’s main income transfer program. According to the Ministry of Development and Social Assistance, it is designed to support low-income families, strengthen access to social rights, and connect income support with broader public policies. Eligibility is mainly based on per-capita household income, and entry depends on registration and updated information in Cadastro Único. The current benefit structure takes household size and composition into account.  
Sources: [MDS - Bolsa Família](https://www.gov.br/mds/pt-br/acoes-e-programas/bolsa-familia), [MDS - Benefit structure](https://www.gov.br/mds/pt-br/acesso-a-informacao/perguntas_frequentes/bolsa-familia-beneficiario/2-quais-sao-os-beneficios)

### What this project is for

This project reproduces a public-policy analytics workflow using `Python + PySpark + Streamlit`:

- territorial trend analysis for Bolsa Família;
- municipality-level comparison of transferred value, beneficiary families, and average benefit;
- operational monitoring of estimated `payment vs withdrawal`;
- identification of municipalities with higher operational gaps;
- future transfer prediction;
- municipality clustering;
- anomaly detection.

### Data source

The territorial layer uses a public open dataset from the state of Alagoas covering `2004-2023`.

The `payment vs withdrawal` layer is **synthetic but calibrated** from the public annual figures, created to reproduce the operational monitoring use case when official transactional downloads are unavailable for automated access.

### What the data represents

The public dataset is aggregated by municipality and year, not at the beneficiary level. It provides annual indicators such as:

- total transferred amount;
- number of beneficiary families;
- average benefit per family.

The project transforms those indicators into:

- a territorial trend layer;
- a municipal profile layer;
- and a monthly operational monitoring layer.

### Stack

- `PySpark`
- `Pandas`
- `scikit-learn`
- `Streamlit`
- `Plotly`

### Techniques used

- data reshaping and pivoting;
- annual growth calculations;
- synthetic operational modeling calibrated from public figures;
- supervised regression with `RandomForestRegressor`;
- unsupervised clustering with `KMeans`;
- anomaly detection with `IsolationForest`.

### Current outputs

- `102` municipalities
- `19` years of history
- `2,448` synthetic operational rows
- average estimated withdrawal rate above `90%`
- regression `R²`: `0.1515`
- selected clusters: `3`
- silhouette score: `0.3158`
- operational anomalies: `74`
