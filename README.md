# HTTP requests to the NASA Kennedy Space Center WWW server

Esse teste de Engenharia de Dados consiste em realizar a leitura de dois datasets de todas as requisições HTTP para o servidor da NASA Kennedy
Space Center WWW na Flórida para um período específico (Julho e Agosto de 1995) e responder questões específicas utilizando Spark e Python.

**Questões Práticas:**

1. Número de hosts únicos
2. Total de Erros 404
3. As 5 URLs que mais causaram erro 404
4. Quantidade de Erros 404 por dia
5. Total de bytes retornados

## Dataset

Dataset consiste de todas as requisições HTTP para o servidor da NASA Kennedy
Space Center WWW na Flórida.

Fonte oficial do dataset: <http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html>

Link para os períodos específicos solicitados:

[Julho 1995, ASCII format, gzip compressed](ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz)
[Agosto 1995, ASCII format, gzip compressed ](ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz)

Logs estão em formato ASCII com uma linha por requisição com as seguintes colunas:

- Host
- Timestamp
- Requisição
- Código do retorno HTTP
- Total de bytes retornados

## Requisitos

O projeto é desenvolvido em Spark e Python e ambos deverão estar instalados e configurados no ambiente de sua preferência. Links para download e instalação:

Apache Spark: <https://spark.apache.org/downloads.html>
Anaconda Python (recomendado): <https://www.anaconda.com/distribution/>

Um guia completo de instalação e configuração para o Windows pode ser encontrado no link: <https://towardsdatascience.com/installing-apache-pyspark-on-windows-10-f5f0c506bea1>

## Execução

Para rodar o script, é necessário que os dataset estejam em uma pasta do diretório. O nome default da pasta é **nasa-data**.

Para submeter o job Spark, executar o comando no terminal/prompt de comando:
```
spark-submit nasa_spark.py
```
Após a execução do job, será criado um arquivo de saída *nasa_data_output.txt* com as respostas das questões propostas.

O exemplo do arquivo de saída para os meses de Julho-1995 e Agosto-1995 está disponível neste repositório.

## Questões Práticas

Dados retirados do arquivo de saída *nasa_data_output.txt*

**1. Número de hosts únicos**
```
137978
```
**2. Total de Erros 404**
```
20901
```
**3. As 5 URLs que mais causaram erro 404**
```
ts8-1.westwood.ts.ucla.edu/images/Nasa-logo.gif
nexus.mlckew.edu.au/images/nasa-logo.gif
203.13.168.24/images/nasa-logo.gif
203.13.168.17/images/nasa-logo.gif
onramp2-9.onr.com/images/nasa-logo.gif
```
**4. Quantidade de Erros 404 por dia**
```
   date                 errors_404              
1995-07-01                316                     
1995-07-02                291                     
1995-07-03                474                     
1995-07-04                359                     
1995-07-05                497                     
1995-07-06                640                     
1995-07-07                570                     
1995-07-08                302                     
1995-07-09                348                     
1995-07-10                398                     
1995-07-11                471                     
1995-07-12                471                     
1995-07-13                532                     
1995-07-14                413                     
1995-07-15                254                     
1995-07-16                257                     
1995-07-17                406                     
1995-07-18                465                     
1995-07-19                639                     
1995-07-20                428                     
1995-07-21                334                     
1995-07-22                192                     
1995-07-23                233                     
1995-07-24                328                     
1995-07-25                461                     
1995-07-26                336                     
1995-07-27                336                     
1995-07-28                 94                     
1995-08-01                243                     
1995-08-03                304                     
1995-08-04                346                     
1995-08-05                236                     
1995-08-06                373                     
1995-08-07                537                     
1995-08-08                391                     
1995-08-09                279                     
1995-08-10                315                     
1995-08-11                263                     
1995-08-12                196                     
1995-08-13                216                     
1995-08-14                287                     
1995-08-15                327                     
1995-08-16                259                     
1995-08-17                271                     
1995-08-18                256                     
1995-08-19                209                     
1995-08-20                312                     
1995-08-21                305                     
1995-08-22                288                     
1995-08-23                345                     
1995-08-24                420                     
1995-08-25                415                     
1995-08-26                366                     
1995-08-27                370                     
1995-08-28                410                     
1995-08-29                420                     
1995-08-30                571                     
1995-08-31                526
```
**5. Total de bytes retornados**
```
61.02 Gb
```
## Questões Teóricas

**1. Qual o objetivo do comando cache em Spark?**

O comando cache() promove a persistência do RDD - Resilient Distributed Datasets em memória. Se trata de uma técnica de 
otimização que salva resultados intermediários na memória para promover a reutilização em próximos passos, portanto, só faria
sentido em casos onde o conjunto de dados será lido mais de uma vez e que o custo de alocar mais dados na memória é melhor 
que o custo de recomputação dos dados.

**2. O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?**

Alguns motivos podem ser elencados:
1. Com relação ao processamento, o Spark o realiza na memória enquanto o MapReduce precisa das operações I/O com o disco, o
que torna esse processamento mais lento.
2. O Spark utiliza o conceito de DAG (Directed Acyclic Graph) avançado e que otimiza os passos necessários para realizar a
tarefa e só efetiva realiza as operações quando necessário (Lazy Evaluation) 
3. O Spark utiliza o conceito de RDD, que em sua essência é uma coleção de objetos que pode ser computados em nodes paralelos
e armazenado em memória.

**3. Qual é a função do SparkContext?**

O SparkContext é um cliente central da aplicação Spark que representa a conexão com o cluster Spark e dá acesso às
funcionalidades e recursos do Spark. Só pode existir um SparkContext por JVM que pode estar ligado a uma ou mais SparkSession.

**4. Explique com suas palavras o que é Resilient Distributed Datasets (RDD)**

RDD - Resilient Distributed Datasets - é a estrutura fundamental de dados do Spark. Essencialmente, é uma coleção distribuída
de objetos que pode ser computada em paralelo. São estruturas imutáveis que podem ser criadas paralelizando uma coleção 
diretamente ou referenciando um dataset.

**5. GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?**

O reduceByKey agrupa os valores da keys de cada máquina antes de realizar o shuffle. Já o GroupByKey realiza o shuffle com
todos os pares key-value antes de agrupá-los. Sendo assim, o GroupByKey transfere mais dados pela rede antes de computar o
resultado final. Sabendo que a rede é um gargalo maior no processo do que a CPU e a memória, geralmente o GroupByKey será mais
lento.

**6. Explique o que o código Scala abaixo faz:**
```
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
                     .map(word =>(word,1))
                     .reduceByKey (_+_)
counts.saveAsTextFile ("hdfs://...")
```

O SparkContext (sc) realiza a leitura de um HDFS (Hadoop File Distributed System) e cria um RDD chamado textFile. Em seguida,
aplica-se o método flatMap com a função de quebra do elemento por espaço, ou seja, um elemento será quebrado em n elementos a
partir do resultado do split. Em seguida, aplica-se um método de map para criação de uma tuple (pares key, value) sendo 
atribuído o valor 1 para cada chave (word). Em seguida, aplica-se o método de reduceByKey para agrupar em somatória os pares
key-value. Finalmente, a ação saveAsTextFile irá executar as tasks e salvar a coleção no mesmo formato distribuído HDFS.
