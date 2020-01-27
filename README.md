# ms-sio

## Cours 1: spark 

- Objectifs:
+ manipuler le dsl de spark
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
- je charge mon dataset

```python
videos = spark.read.option('header','true').option('inferSchema','true').csv('/data/raw/FRvideos.csv')
```
- je decouvre mon dataset

```python
videos.count()

videos.show()

videos.show(50)

videos.show(10,False)

```

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

- lire le dataset en json

```python
categories = spark.read.option('multiline','true').json('/data/raw/FR_category_id.json')

categories.count()

categories.show()

categories.printSchema()

```

- je transforme les données pour qu'elles soient plus manipuable:

```python
from pyspark.sql.functions import *

categories = categories.select(explode('items'))

categories = categories.select('col.id','col.snippet.title')

categories = categories.withColumnRenamed('title','category_title')
```

```python

df = videos.join(categories.hint('broadcast'),videos.category_id == categories.id,'inner')

```

__note__ : voir par [ici](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#) pour la syntaxe


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
 
- faire exploser la memoire:

```python 
cs = df.crossJoin(df)
cs = df.crossJoin(df.sample(0.009))
```

- Comment resoudre ce genre de situation?
    + Comprendre le Memory Model de spark
    
    + de quelle resources je dispose sur mon cluster
        + Memoire 
        + CPU
        + Noeuds
    + quelle quantité de données je vais manipuler
        + Tailles
        + Partitions
    
- Comment je vais sauvegarder mon resultat:
    + csv
    + json
    + parquet
    + avro
    + ORC
    + Protobuff (TFRecords)    
    
- Comment je lance mon job en production?

```shell
   docker run --rm -ti -v ~/cours-hdp2/datasets:/data -v $(pwd)/data-ingestion-job/src:/src  --entrypoint bash stebourbi/sio:pyspark
   
   /spark-2.4.4-bin-hadoop2.7/bin/spark-submit /src/process.py -v /data/raw/FRvideos.csv -c /data/raw/FR_category_id.json -o /data/output00
```

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
kafka-topics --bootstrap-server localhost:9092 --create --topic topic01 --partitions 1 --replication-factor 1

kafka-console-producer --broker-list localhost:9092  --topic topic01

kafka-console-consumer --bootstrap-server  localhost:9092  --topic videos --from-beginning
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
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka1:19092").option("subscribe", "videos").option("startingOffsets", "latest").load()
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
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka1:19092").option("subscribe", "videos").option("startingOffsets", "latest").load()
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






 

