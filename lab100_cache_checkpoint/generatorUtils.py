import logging
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType)
from pyspark.sql import (SparkSession, functions as F)
from random import randint
import datetime

now = datetime.datetime.now()

fnames = [
    "John", "Kevin", "Lydia", "Nathan",
    "Jane", "Liz", "Sam", "Ruby", "Peter", "Rob", "Mahendra", "Noah",
    "Noemie", "Fred", "Anupam", "Stephanie", "Ken", "Sam", "Jean-Georges",
    "Holden", "Murthy", "Jonathan", "Jean", "Georges", "Oliver"
]

lnames = [
    "Smith", "Mills", "Perrin", "Foster",
    "Kumar", "Jones", "Tutt", "Main", "Haque", "Christie", "Karau",
    "Kahn", "Hahn", "Sanders"
];

articles = [ "The", "My", "A", "Your", "Their" ];

adjectives = [
    "", "Great", "Beautiful", "Better",
    "Worse", "Gorgeous", "Terrific", "Terrible", "Natural", "Wild"
];

nouns = [ "Life", "Trip", "Experience", "Work","Job", "Beach" ];

lang = [ "fr", "en", "es", "de", "it", "pt" ];

daysInMonth =  [ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 ];


schema = StructType([
     StructField('name', StringType(), False),
     StructField('title', StringType(), False),
     StructField('rating', IntegerType(), False),
     StructField('year', IntegerType(), False),
     StructField('lang', StringType(), False)
 ])

def createDataframe(spark: SparkSession, recordCount: int):

    log = logging.getLogger(__name__)

    inc = 500000; recordCreated = 0
    df = None

    while (recordCreated < recordCount):

        recordInc = inc;
        if (recordCreated + inc > recordCount):
            recordInc = recordCount - recordCreated;

        rows: List =[]
        for j in range(recordInc):
            rows.append (
                (
                "{0} {1}".format(get_first_name(),get_last_name()),
                get_title(),
                get_rating(),
                get_recent_years(25),
                get_lang()
                )
            )

        if df == None:
            df = spark.createDataFrame(rows, schema);
        else:
            df = df.union(spark.createDataFrame(rows, schema));

        recordCreated = df.count();

    return df

def get_random_int(i: int):
    return randint(0, i);

def get_lang():
    return lang[get_random_int( len(lang) - 1) ];

def get_rating():
    return get_random_int(3) + 3;

def get_first_name():
    return fnames[ get_random_int( len(fnames) - 1) ];

def get_last_name():
    return lnames[ get_random_int(len(lnames) - 1) ];

def get_article():
    return articles[get_random_int( len(articles) - 1)];

def get_adjective():
    return adjectives[get_random_int( len(adjectives) - 1)];

def get_noun():
    return nouns[get_random_int( len(nouns) - 1)];

def get_title():
    return "{0} {1} {2}".format( get_article(), get_adjective(), get_noun());

def get_random_SSN():
    return "{0}{1}{2}-{3}{4}-{5}{6}{7}{8}"\
    .format(
        get_random_int(10),
        get_random_int(10),
        get_random_int(10),
        get_random_int(10),
        get_random_int(10),
        get_random_int(10),
        get_random_int(10),
        get_random_int(10),
        get_random_int(10)
    )

def get_recent_years(i: int):
    return now.year - get_random_int(i);


if __name__ == "__main__":

    log = logging.getLogger(__name__)

    spark = SparkSession\
        .builder\
        .appName("net.jgp.books.spark.ch16.lab100_cache_checkpoint.RecordGeneratorUtils") \
        .master("local[*]")\
        .getOrCreate()

    spark.sparkContext.setLogLevel('error')

    df = createDataframe(spark, 10000)

    df.show()
    df.printSchema()
    spark.stop()
