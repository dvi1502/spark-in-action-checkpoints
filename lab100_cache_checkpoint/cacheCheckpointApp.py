import logging
import enum
from time import sleep
from pyspark.sql import (SparkSession, functions as F)
from  generatorUtils import createDataframe

class Mode(enum.Enum):
    NO_CACHE_NO_CHECKPOINT = 1
    CACHE = 2
    CHECKPOINT = 3
    CHECKPOINT_NON_EAGER = 4

def current_time():
    from datetime import datetime
    return datetime.now()

def process_dataframe( spark: SparkSession, recordCount: int, mode: Mode ):

    df = createDataframe(spark, recordCount);
    t0 = current_time()
    topDf = df.filter(F.col("rating") == F.lit(5));

    if mode == Mode.CACHE:
        topDf = topDf.cache()
    elif mode == Mode.CHECKPOINT:
        topDf = topDf.checkpoint(True)
    elif mode == Mode.CHECKPOINT_NON_EAGER:
        topDf = topDf.checkpoint(False)

    langLst = topDf\
        .groupBy("lang")\
        .count()\
        .orderBy("lang")\
        .collect();

    yearLst = topDf\
        .groupBy("year")\
        .count()\
        .orderBy(F.col("year").desc())\
        .collect();

    t1 = current_time()

    return t1 - t0


if __name__ == "__main__":

    log = logging.getLogger(__name__)

    spark = SparkSession\
        .builder\
        .appName("net.jgp.books.spark.ch16.lab100_cache_checkpoint.CacheCheckpointApp")\
        .config("spark.executor.memory", "70g")\
        .config("spark.driver.memory", "50g")\
        .config("spark.memory.offHeap.enabled", True)\
        .config("spark.memory.offHeap.size", "16g")\
        .master("local[*]")\
        .getOrCreate()

    spark.sparkContext.setLogLevel('warn')
    spark.sparkContext.setCheckpointDir('/tmp/checkpoints')

    # Specify the number of records to generate
    recordCount = 10000;

    # Create and process the records without cache or checkpoint
    t0 = process_dataframe(spark, recordCount, Mode.NO_CACHE_NO_CHECKPOINT);

    # Create and process the records with cache
    t1 = process_dataframe(spark, recordCount, Mode.CACHE);

    # Create and process the records with a checkpoint
    t2 = process_dataframe(spark, recordCount, Mode.CHECKPOINT);

    # Create and process the records with a checkpoint
    t3 = process_dataframe(spark, recordCount, Mode.CHECKPOINT_NON_EAGER);

    log.warning("Processing times:");
    log.warning("Without cache ............... {0} ms".format(t0))
    log.warning("With cache .................. {0} ms".format(t1))
    log.warning("With checkpoint ............. {0} ms".format(t2))
    log.warning("With non-eager checkpoint ... {0} ms".format(t3))

    spark.stop()
