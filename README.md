**GCP Engineer Dotz**

GCP Engineer Dotz é a resolução do problema apresentando por uma empresa
que vende máquinas industriais. O desafio é executar a integração de
dados em formato CSV e apresentar visões em base estruturada.

**Arquitetura**:  

![](mdMediaFolder\media\image1.png){width="6.167530621172354in"
height="2.3645833333333335in"}

Figura 1 - imagem modificada, fonte
https://towardsdatascience.com/apache-beam-pipeline-for-cleaning-batch-data-using-cloud-dataflow-and-bigquery-f9272cd89eba

**Modelo**:

![](mdMediaFolder\media\image2.emf){width="5.125in"
height="3.5701498250218724in"}

Figura 2- fonte própria

## Passo a passo

Configurar Ambiente:

Necessário criar projeto via console e habilitar os serviços Cloud
Storage, DataFlow, DataFlow API e BigQuery.

Abra o Cloud Shell Editor e execute os comando:

()\$ pip3 install \--upgrade virtualenv \--user

()\$ python3 -m virtualenv env

()\$ source env/bin/activate

()\$ pip3 install \--quiet apache-beam\[gcp\]

()\$ gsutil mb gs://dotztest

Acesse cloud Storage e crie os folders:

-dotztest/rawzn

Nesse folder, suba os arquivos price_quote.csv, Bill_of_materials.csv e
comp_boss.csv

-dotztest/srcDtFlow

Nesse folder, suba os fontes DataFlow: bill_of_materials.py,
comp_boss.py, price_quote.py

-dotztest/trustzn

Executar teste de carga bill_of_materials.csv:

No Cloud Shell Editor e execute os comandos:

()\$ gsutil cp gs://dotztest/srcDtFlow/bill_of_materials.py
bill_of_materials.py

()\$ python3 bill_of_materials.py \\

\--project \\

my-project-1536110405564 \\

\--runner DataflowRunner \\

\--temp_location \\

gs://dotztest/srcDtFlow/temp \\

\--output \\

gs://dotztest/trustzn/output \\

\--job_name dataflow-bill \\

\--region southamerica-east1

Demais testes vc encontra no arquivo command_cloud_shell.txt
