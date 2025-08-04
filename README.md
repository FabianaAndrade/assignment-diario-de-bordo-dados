# assignment-diario-de-bordo-dados

Esse fluxo aborda uma situação-problema para criar um pipeline de dados com capacidade para ler dado origem, persistencia na camada bronze, transformação na camada silver e consolidação análitica na camada gold.

A base de dados é sobre corridas em aplicativos de transporte. O arquivo fonte original possui possui colunas que descrevem momento que a corrida iniciou, quando 
acabou, categoria da viagem (pessoal ou negocio), local de embarque do passageiro, local de desembarque, proposito da viagem  (Reunião ou não) e distância percorrida.

Dessa forma, foi construido um pipeline de dados em spark que cobre aspectos desde a captura do arquivo fonte (csv) até a criação da camada gold. O processo foi desenvolvido em linguagem python e implementado com docker para rodar isolado com serviço em um conteiner. 

## Arquitetura Macro
<img width="1672" height="559" alt="Sem título-2025-07-02-2113(2)" src="https://github.com/user-attachments/assets/8db9d784-6544-4c6a-91d4-5210e8ec92d4" />

Arquivo origem: conjunto de dados com as informações fonte

camada bronze: tabela que possui a leitura dos dados origem perisistidos em parquet e particionados por data

camada silver: tabela que manipula os dados bronze, realizando formações em tipos de dados e padronização de valores faltantes

camada gold: tabela que agrega, conforme a regra de negócio, as inforamações da camada silver, gerando uma base pronta para reports.

notebooks python: utilizados para entender os dados fonte do csv e visualizar os resultados gravados na camada gold.

```text 
/app
│
├── data/
│   ├── inputs/
|       └── info_transportes.csv    # arquivo origem
│   └── outputs/                    # resultados persisitidos (parquet) por camada
|       ├── raw/
|       ├── silver/
|       └── gold/
│ 
├── pipeline/
│   ├── __init__.py
│   ├── data_aggregation.py         # consolidacao da base gold
|   ├── data_reading.py             # leituras csv e parquet
|   ├── data_transformation.py      # transformacoes nos dados
|   └── writter.py                  # persistencias nas camadas
│
│
├── utils/
│   ├── __init__.py
│   └──  Logger.py                   # Classe utilitária para logging
│ 
├── tests/
│   ├── __init__.py
|   ├── test_aggregation.py         # teste na logica de agregacao
|   ├── test_reader.py              # teste na logica de leitura
    ├── test_tranformer.py          # teste na logica de transformacao
│   └── conftest.py                 # configs testes unitarios
│
├── main.py                         # Executor
|
├── exploration/
│   ├── data_exploration_csv.ipynb   # analise incial dos dados origem
│   └── data_exploration_gold.ipynb  # visualizacao dos resultados da camada gold
│
├── requirements.txt                # modulos requeridos
├── Dockerfile.tests                # Dockerfile com as definicoes para o ambiente de teste
├── Dockerfile                      # Dockerfile para o app
└── docker-compose.yml              # Orquestração dos serviços com Docker
```
# Utilização

Tenha previamente instalado:
```text
Docker e Docker-compose
```

1. Clone o repo
```text
 git clone https://github.com/FabianaAndrade/assignment-diario-de-bordo-dados.git
```
2. Defina as váriaveis de ambientes
Crie um arquivo .env na raiz do projeto com os valores das variaveis

```text
SOURCE_DATA_FOLDER =  # caminho dos arquivo csv fonte
TARGET_DATA_FOLDER =  # nome da pasta onde os dados serao persistidos
CSV_SEP =             # tipo de separador do csv
COLUMN_DAT_REF =      # nome da coluna base para considerar dat ref
PART_COLUMN_NAME =    # nome para a coluna dat ref

```
Caso não sejam definidas serão assumidos valores defaults

3. Inicie os containers
```text
docker-compose up --build 
```
4. Desligando os serviços
```text
docker-compose down
```
5. Execucao de testes unitarios
```text
docker build -f Dockerfile.tests -t tests_pipeline .
docker run --rm tests_pipeline
```


