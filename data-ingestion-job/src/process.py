# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse

def main():
    """
    Cette methode est le point d'entrée de mon job.
    Elle va essentiellement faire 3 choses:
        - recuperer les arguments passés via la ligne de commande
        - creer une session spark
        - lancer le traitement
    Le traitement ne doit pas se faire dans la main pour des soucis de testabilité.
    """
    parser = argparse.ArgumentParser(
        description='Discover driving sessions into log files.')
    parser.add_argument('-v', "--videos_file", help='Videos input file', required=True)
    parser.add_argument('-c', "--categories_file", help='Categories input file', required=True)
    parser.add_argument('-o', '--output', help='Output file', required=True)

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    process(spark,args.videos_file,args.categories_file,args.output)


def process(spark, videos_file, categories_file, output):
    """
    Contient les traitements qui permettent de lire,transformer et sauvegarder mon resultat.
    :param spark: la session spark
    :param videos_file:  le chemin du dataset des videos
    :param categories_file: le chemin du dataset des categories
    :param output: l'emplacement souhaité du resultat
    """
    videos = spark.read.option('header', 'true').option('inferSchema', 'true').csv(videos_file)
    videos = videos.filter(videos.category_id.rlike('\\d*')).withColumn('category_id',
                                                                        videos.category_id.cast('integer'))

    categories = spark.read.option('multiline', 'true').json(categories_file)

    categories = categories.select(explode('items'))

    categories = categories.select('col.id', 'col.snippet.title')

    categories = categories.withColumnRenamed('title', 'category_title')

    df = videos.join(categories.hint('broadcast'), videos.category_id == categories.id, 'inner')

    df.write.parquet(output)


if __name__ == '__main__':
    main()
