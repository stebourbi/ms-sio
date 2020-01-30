# ms-sio

## Cours 1: spark 

- Objectifs:
+ manipuler le dsl de spark (Dataframe API)
+ comprendre le concept de RDD
+ comprendre le modele de memoire
+ voir comment configurer un job spark 
+ voir comment lancer un job spark 

- preparer le dataset:

télécharger le dataset via cette page: https://www.kaggle.com/datasnaek/youtube-new

le fichier zip télécharger doit etre a ce path : `~/Downloads/youtube-new.zip`.

```bash
make prepare-dataset
``` 

- lancer pyspark :

```bash
make run-pyspark
``` 

Spark est un outil permettant de manipuler les données via différentes  "API": [RDD](http://spark.apache.org/docs/latest/rdd-programming-guide.html), [Dataframe](http://spark.apache.org/docs/latest/sql-programming-guide.html), [SQL](http://spark.apache.org/docs/latest/api/sql/index.html) et differents languages (essentiellement [Scala](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package) et [Python](http://spark.apache.org/docs/latest/api/python/index.html))

Dans ce cours on utlisera l'API Dataframe via son implémentation en  Python.

Pour faciliter l'introspection dans un dataset ou pour modéliser un traitement ou simplement le tester Spark offre des [REPL](https://en.wikipedia.org/wiki/Read%E2%80%93eval%E2%80%93print_loop), dans ce cours on a utilisé `pyspark` qui est la version en python.


- je charge mon dataset, pour cela on utilise le concept de [Datasource](http://spark.apache.org/docs/latest/sql-data-sources.html).

```python
videos = spark.read.option('header','true').option('inferSchema','true').csv('/data/raw/FRvideos.csv')
```
- je decouvre mon dataset.

```python
videos.count()

videos.show()

videos.show(50)

videos.show(10,False)

```
- Chaque dataframe possede un schema:

```python 
videos.printSchema()
```
- chercher et corriger les erreurs d'inference de schema:


```python
videos.select('category_id').show()
videos.select('category_id').sample(0.1).distinct().show()
videos.select('category_id').filter(videos.category_id.rlike('\\d*')).count()

videos = videos.filter(videos.category_id.rlike('\\d*')).withColumn('category_id',videos.category_id.cast('integer'))
```
__Remarque__ ici on a fait que des operations de type `transformation` (select, filter, where, ...). Spark va juste les prendre en compte sans les éxécuter immédiatement.
Pour provoquer l'execution d'un job, il faut soumettre une `action` (write, save, count, show).

En general une transformation prend une RDD/DataFrame en entrée et donne une autre en sortie. Tandis qu'une action donne un resultat autre qu'un RDD/DataFrame.
Par exemple: 

- `count()` = f(RDD) => Long  
- `show()` = f(RDD) => String
- `write.parquet()` = f(RDD => "Fichiers.parquet"  
 

- lire le dataset en json.

```python
categories = spark.read.option('multiline','true').json('/data/raw/FR_category_id.json')

categories.count()

categories.show()

categories.printSchema()

```

__Attention__ dans le monde spark quand on parle de JSON on s'attend a avoir du [Json Lines](http://jsonlines.org/)

- je transforme les données pour qu'elles soient plus manipuable:

```python
from pyspark.sql.functions import *

categories = categories.select(explode('items'))

categories = categories.select('col.id','col.snippet.title')

categories = categories.withColumnRenamed('title','category_title')
```

- Je fait une jointure entre les deux datasets:

```python

df = videos.join(categories.hint('broadcast'),videos.category_id == categories.id,'inner')

```

Je me sert du fait que le dataset des catégories a une petite taille pouvant etre supportée par la mémoire du Driver et envoyée a tous les executors (via le réseau!!) assez rapidement;
pour faire une optimisation de la jointure dites: [`Broadcast join`](https://spark.apache.org/docs/latest/sql-performance-tuning.html#broadcast-hint-for-sql-queries)

Dans le cas ou je fait une jointure de deux datasets de tailles importantes il faut travailler sur la definition des partitions des deux datasets ([co-partitionning](https://www.howtobuildsoftware.com/index.php/how-do/bbjm/apache-spark-spark-streaming-rdd-does-a-join-of-co-partitioned-rdds-cause-a-shuffle-in-apache-spark)) qui doit pouvoir limiter l'impact des opération d'echanges de données via le reseau (shuffle).
Un bon choix de partitionnement necessite une bonne connaissance des datasets (nature, connaissance du domaine, tailles) et aussi des ressources a dispositions;
 
voir cette article: https://techmagie.wordpress.com/2015/12/19/understanding-spark-partitioning/

### comprendre la gestion de la mémoire:


- sur un navigateur ouvrir l'inteface spark UI http://localhost:4040

```python
 df.cache() 
 df.count() # action pour forcer la mise en cache

```
- voir l'onglet storage.

- autre facon de mettre en cache:

```python
df.persist(pyspark.StorageLevel.MEMORY_ONLY) # voir https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence

```

__A retenir__

- La memoire est l'une des ressources avec laquelle on inter-agit le plus en utilisant/optimisant spark.
- Quand on parle de mémoire ne surtout pas confondre:
    + La Memoire des machines sur lesquelles tournent mes executeurs spark. Cette memoire ne doit pas etre entierement donner a spark.
    + La Memoire totale du process JVM qui représente l'executeur spark, qui ne peut etre entierement dédiée a nos traitements;  
    + La fraction de mémoire gérer par le [Memory Manager de Spark](https://github.com/apache/spark/blob/branch-1.6/core/src/main/scala/org/apache/spark/memory/MemoryManager.scala)
    + Cette derniere est divisée en deux parties l'une pour l'execution (jointure, sort, aggregation,...) et l'autre pour le stockage (cache).
    + C'est sur cette fraction qu'on va le plus souvent agir pour optimiser l'usage de nos resources.
  
         
Lire cet article qui détaille bien le modele de mémoire de spark: [Spark Memory Management](https://medium.com/@muditnagar/spark-memory-management-v1-6-0-8c9178970652)

Mais aussi:
- la documentation : https://spark.apache.org/docs/latest/tuning.html#memory-management-overview
- et surtout le code source en commenceant par l'implémentation du [UnifiedMemoryManager](https://github.com/apache/spark/blob/branch-1.6/core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala)
     

__A NE JAMAIS JAMAIS JAMAIS OUBLIER__ : Spark n'est ni conçu ni pensé pour charger ENTIEREMENT un dataset en mémoire. Un dataset est représenté par une RDD qui est partitionnée en plusieurs blocks/partitions.
Il faut qu'au moins la taille "d'un seul" block/partition "puisse tenir en mémoire".   
    
    
    
- Comment je vais sauvegarder mon resultat:

```python

    df.write.parquet('/data/my-joined-dataset')
```

Je choisis de [sauvegarder mon dataset en parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) car c'est un format de stockage trés optimisé pour un usage classique de spark.
 

- Comment je lance mon job en production?

voir d'abord [comment j'ai ré-écrit mes traitements](./data-ingestion-job/src/process.py) sous forme d'un programme python plus structuré que  les commandes que j'ai jusqu'ici lancé avec pyspark.

Pour lancer un job spark j'utilise la commande [spark-submit](http://spark.apache.org/docs/latest/submitting-applications.html) et ceux quelque soit le language utilisé.

Le lancement d'un job consiste concrétement a [demander au scheduler du cluster spark](http://spark.apache.org/docs/latest/cluster-overview.html) de lancer le traitement soumis.

Le scheduler le plus utilisé pour le moment en production est [YARN](http://spark.apache.org/docs/latest/running-on-yarn.html)

```shell
   docker run --rm -ti -v ~/cours-hdp2/datasets:/data -v $(pwd)/data-ingestion-job/src:/src  --entrypoint bash stebourbi/sio:pyspark
   
   /spark-2.4.4-bin-hadoop2.7/bin/spark-submit /src/process.py -v /data/raw/FRvideos.csv -c /data/raw/FR_category_id.json -o /data/output00
```

Je peux aussi changer certaines configurations au lancement de mon job, par exemple celle de la mémoire:

```shell
    /spark-2.4.4-bin-hadoop2.7/bin/spark-submit --conf spark.executor.memory=8G /src/process.py -v /data/raw/FRvideos.csv -c /data/raw/FR_category_id.json -o /data/output00
```

D'autres parametres "importants" sont souvent passé au lancement d'un job:

```shell
    /spark-2.4.4-bin-hadoop2.7/bin/spark-submit --name videos-integration \
     --master yarn \ # YARN reste le scheduler le plus utilisé pour des cluster de production
     --deploy-mode cluster \ # quand on lance en mode `cluster` le driver sera lancé sur une machine du cluster et gérer par le scheduler. Contrairement au mode `client` 
     --conf spark.executor.memory=12G \
     --conf spark.executor.cores=4 \ # nombre de cpu par executor ce qui lui permet d'executer 4 taches en paralleles qui se partagerons la memoire d'execution. 
     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
     --conf spark.shuffle.service.enabled=true \
     --conf spark.dynamicAllocation.enabled=true \
      /src/process.py -v /data/raw/FRvideos.csv -c /data/raw/FR_category_id.json -o /data/output00
```

Voici la liste [des configurations](http://spark.apache.org/docs/latest/configuration.html). Attention certaines configurations dependent du scheduler utilisé!

      
__a voir/aller plus loin__
  
  - voir par [ici](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#) pour la syntaxe
  - https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html
  - les fondements: 
    + [Resilient Distributed Datasets: A Fault-Tolerant Abstraction for
In-Memory Cluster Computing](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)
    + [Spark SQL: Relational Data Processing in Spark](https://amplab.cs.berkeley.edu/wp-content/uploads/2015/03/SparkSQLSigmod2015.pdf)



## Cours 2: streaming 

Objectifs:
 - comprendre les concepts
 - manipuler un backend de stream : kafka
 - faire des traitement en streaming avec spark structured streaming

kafka:
=====================


lancer une instance:

```bash
docker-compose -f zk-single-kafka-single.yml up
```

s'y connecter et faire des manips:

```bash 
kafka-topics --bootstrap-server localhost:9092 --create --topic topic03 --partitions 3 --replication-factor 1

kafka-console-producer --broker-list localhost:9092  --topic topic03

kafka-console-consumer --bootstrap-server  localhost:9092  --topic topic03
```

lancer un spark-shell (pyspark) sur un container docker qui voit le broker kafka

```bash
docker run --rm -ti -v ~/cours-hdp2/datasets:/data --network docker_default -p 4040:4040 stebourbi/sio:pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4
```

Lire depuis le stream/topic kafka
```bash
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka1:19092").option("subscribe", "topic01").load()

df.isStreaming
df.printSchema()

df.count()
df.show()

query = df.writeStream.outputMode("update").format("console").start()

query.awaitTermination()
```

inserer des messages dans le topic

```bash
cat <<EOF >data
2020-01-26 10:12:15,ligne1,1
2020-01-26 10:13:12,ligne2,2
2020-01-26 10:13:13,ligne3,3
2020-01-26 10:14:16,ligne4,4
EOF

kafka-console-producer --broker-list localhost:9092  --topic topic01 < data

```
Les questions qu'on se pose pour un traitement en streaming:

 - Quel traitement?
 - A quelle periode/fenetre s'applique le traitement?
 - Quand est ce qu'on emet le resultat?
 - Comment on restitue le resultat?


- ajouter le temps de capture de la données

```python

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka1:19092").option("subscribe", "topic01").option("startingOffsets", "latest").load()
df = df.select(df.value.cast("string"),df.timestamp.alias('capture_time'))
query = df.writeStream.outputMode("update").format("console").option('truncate', 'false').start()
query.awaitTermination()
```

- ajouter le temps de l'evenement


```python
from pyspark.sql.functions import *
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka1:19092").option("subscribe", "topic01").option("startingOffsets", "latest").load()
df = df.select(df.value.cast("string"),df.timestamp.alias('capture_time'))
df = df.withColumn('event_time',to_timestamp(split(df.value,',')[0]))
query = df.writeStream.outputMode("update").format("console").option('truncate', 'false').start()
query.awaitTermination()
```

- ajouter le temps de traitement


```python
from pyspark.sql.functions import *
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka1:19092").option("subscribe", "topic01").option("startingOffsets", "latest").load()
df = df.select(df.value.cast("string"),df.timestamp.alias('capture_time'))
df = df.withColumn('processing_time',current_timestamp())
df = df.withColumn('event_time',to_timestamp(split(df.value,',')[0]))
query = df.writeStream.outputMode("update").format("console").option('truncate', 'false').start()
query.awaitTermination()
```

- sortir la valeur de l'evenement

```python
from pyspark.sql.functions import *
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka1:19092").option("subscribe", "topic01").option("startingOffsets", "latest").load()
df = df.select(df.value.cast("string"),df.timestamp.alias('capture_time'))
df = df.withColumn('processing_time',current_timestamp())
df = df.withColumn('event_time',to_timestamp(split(df.value,',')[0]))
df = df.withColumn('value',split(df.value,',')[1])
query = df.writeStream.outputMode("update").format("console").option('truncate', 'false').start()
query.awaitTermination()
```

- traiter par fenetre de temps:

```python
from pyspark.sql.functions import *
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka1:19092").option("subscribe", "topic01").option("startingOffsets", "latest").load()
df = df.select(df.value.cast("string"),df.timestamp.alias('capture_time'))
df = df.withColumn('processing_time',current_timestamp())
df = df.withColumn('event_time',to_timestamp(split(df.value,',')[0]))
df = df.withColumn('value',split(df.value,',')[1])
df = df.groupBy(window(df.capture_time,"10 minutes","10 minutes"),df.value).count()
query = df.writeStream.outputMode("update").format("console").option('truncate', 'false').start()
query.awaitTermination()
```


- gerer les evenement en retard:

```python
from pyspark.sql.functions import *
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka1:19092").option("subscribe", "topic03").option("startingOffsets", "latest").load()
df = df.select(df.value.cast("string"),df.timestamp.alias('capture_time'))
df = df.withColumn('processing_time',current_timestamp())
df = df.withColumn('event_time',to_timestamp(split(df.value,',')[0]))
df = df.withColumn('value',split(df.value,',')[1])
df = df.withWatermark("capture_time", "15 minutes") 
df = df.groupBy(window(df.capture_time,"10 minutes","10 minutes"),df.value).count()
query = df.writeStream.outputMode("update").format("console").option('truncate', 'false').start()
query.awaitTermination()
```


___ a retenir__

windowing:
 - fixed window: simple decoupage en periode egale (sliding avec pas == periode)
 - sliding window: definie par un pas et une periode (souvent pas >= periode)
 - session: depend de la presence ou pas d'un pattern dans la donée
le windowing peut se baser sur :
 - event time
 - processing time 
 - capture time   



- decider du temps de traitement:

```python
from pyspark.sql.functions import *
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka1:19092").option("subscribe", "topic3").option("startingOffsets", "latest").load()
df = df.select(df.value.cast("string"),df.timestamp.alias('capture_time'))
df = df.withColumn('processing_time',current_timestamp())
df = df.withColumn('event_time',to_timestamp(split(df.value,',')[0]))
df = df.withColumn('value',split(df.value,',')[1])
df = df.withWatermark("capture_time", "15 minutes") 
df = df.groupBy(window(df.capture_time,"10 minutes","10 minutes"),df.value).count()
query = df.writeStream.trigger(processingTime='30 seconds').outputMode("complete").format("console").option('truncate', 'false').start()
query.awaitTermination()
```
__a retenir__: la notion completude de donées par rapport au frontiere des windows:
 - watermark: la limite aprés laquelle on considere/suppose que tte les données sont parvenues
 - triggering: quand on considere que les events/données relative a une window sont constituée et prete au processing
la restitution du resultat:
    - complete
    - 'append'
    - mise a jour






 

