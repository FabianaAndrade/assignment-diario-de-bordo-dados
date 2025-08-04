# assignment-diario-de-bordo-dados

Esse fluxo aborda uma situação-problema para criar um pipeline de dados com capicidade para ler dado origem, persistencia na camada bronze, transformação na camada silver e consolidação análitica na camada gold.

A base de dados é sobre corridas em aplicativos de transporte. O arquivo fonte original possui possui colunas que descrevem momento que a corrida iniciou, quando 
acabou, categoria da viagem (pessoal ou negocio), local de embarque do passageiro, local de desembarque, proposito da viagem  (Reunião ou não) e distância percorrida.

Dessa forma, foi construido um pipeline de dados em spark que cobre aspectos desde a captura do arquivo fonte (csv) até a criação da camada gold.

