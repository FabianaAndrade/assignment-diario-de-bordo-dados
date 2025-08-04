# assignment-diario-de-bordo-dados

Esse fluxo aborda uma situação-problema para criar um pipeline de dados com capicidade para ler dado origem, persistencia na camada bronze, transformação na camada silver e consolidação análitica na camada gold.

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

