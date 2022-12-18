import logging
import enum
from time import sleep
from pyspark.sql import (SparkSession, functions as F, DataFrame)


class Mode(enum.Enum):
    NO_CACHE_NO_CHECKPOINT = 1
    CACHE = 2
    CHECKPOINT = 3
    CHECKPOINT_NON_EAGER = 4

def current_time():
    from datetime import datetime
    return datetime.now()

def process(df: DataFrame, mode: Mode ):

    log = logging.getLogger(__name__)
    t0 = current_time()

    df = df\
        .orderBy(F.col("CAPITAL").desc())\
        .withColumn("WAL-MART", F.when(F.col("WAL-MART").isNull(), 0).otherwise(F.col("WAL-MART")))\
        .withColumn("MAC", F.when(F.col("MAC").isNull(), 0).otherwise(F.col("MAC")))\
        .withColumn("GDP", F.regexp_replace(F.col("GDP"), ",", "."))\
        .withColumn("GDP", F.col("GDP").cast("float"))\
        .withColumn("area", F.regexp_replace(F. col("area"), ",", ""))\
        .withColumn("area", F.col("area").cast("float"))\
        .groupBy("STATE")\
        .agg(
            F.first("CITY").alias("capital"),
            F.sum("IBGE_RES_POP_BRAS").alias("pop_brazil"),
            F.sum("IBGE_RES_POP_ESTR").alias("pop_foreign"),
            F.sum("POP_GDP").alias("pop_2016"),
            F.sum("GDP").alias("gdp_2016"),
            F.sum("POST_OFFICES").alias("post_offices_ct"),
            F.sum("WAL-MART").alias("wal_mart_ct"),
            F.sum("MAC").alias("mc_donalds_ct"),
            F.sum("Cars").alias("cars_ct"),
            F.sum("Motorcycles").alias("moto_ct"),
            F.sum("AREA").alias("area"),
            F.sum("IBGE_PLANTED_AREA").alias("agr_area"),
            F.sum("IBGE_CROP_PRODUCTION_$").alias("agr_prod"),
            F.sum("HOTELS").alias("hotels_ct"),
            F.sum("BEDS").alias("beds_ct"))\
        .withColumn("agr_area", F.expr("agr_area / 100"))\
        .orderBy(F.col("STATE"))\
        .withColumn("gdp_capita", F.expr("gdp_2016 / pop_2016 * 1000"))

    if mode == Mode.CACHE:
        df = df.cache()
    elif mode == Mode.CHECKPOINT:
        df = df.checkpoint(True)
    elif mode == Mode.CHECKPOINT_NON_EAGER:
        df = df.checkpoint(False)
    df.show(5);

    #
    t1 = current_time()
    log.warning(f"Aggregation (ms) .................. {t1 - t0}");

    # Regions per population
    log.warning("***** Population")
    popDf = df\
        .drop(
            "area", "pop_brazil", "pop_foreign", "post_offices_ct",
            "cars_ct", "moto_ct", "mc_donalds_ct", "agr_area", "agr_prod",
            "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area",
            "gdp_2016")\
        .orderBy(F.col("pop_2016").desc())
    popDf.show(30)

    t2 = current_time();

    log.warning(f"Population (ms) ................... {(t2 - t1)}")
    #
    # Regions per size in km2
    log.warning("***** Area (squared kilometers)");
    areaDf = df\
        .withColumn("area", F.round(F.col("area"), 2))\
        .drop(
            "pop_2016", "pop_brazil", "pop_foreign", "post_offices_ct",
            "cars_ct", "moto_ct", "mc_donalds_ct", "agr_area", "agr_prod",
            "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area",
            "gdp_2016")\
        .orderBy(F.col("area").desc())
    areaDf.show(30)

    t3 = current_time()
    log.warning(f"Area (ms) ......................... {(t3 - t2)}")
    #
    # McDonald's per 1m inhabitants
    log.warning("***** McDonald's restaurants per 1m inhabitants");
    mcDonaldsPopDf = df\
        .withColumn("mcd_1m_inh", F.expr("int(mc_donalds_ct / pop_2016 * 100000000) / 100"))\
        .drop(
            "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
            "moto_ct", "area", "agr_area", "agr_prod", "wal_mart_ct",
            "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016"
        ).orderBy(F.col("mcd_1m_inh").desc())
    mcDonaldsPopDf.show(5)

    t4 = current_time()
    log.warning(f"Mc Donald's (ms) .................. { (t4 - t3)}")

    # Walmart per 1m inhabitants
    log.warning("***** Walmart supermarket per 1m inhabitants")
    walmartPopDf = df\
        .withColumn("walmart_1m_inh",  F.expr("int(wal_mart_ct / pop_2016 * 100000000) / 100"))\
        .drop(
            "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
            "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016")\
        .orderBy(F.col("walmart_1m_inh").desc())
    walmartPopDf.show(5)

    t5 = current_time()
    log.warning(f"Walmart (ms) ...................... {t5 - t4}")

    # GDP per capita
    log.warning("***** GDP per capita")
    gdpPerCapitaDf = df\
        .drop(
            "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
            "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area")\
        .withColumn("gdp_capita", F.expr("int(gdp_capita)"))\
        .orderBy(F.col("gdp_capita").desc());
    gdpPerCapitaDf.show(5)
    t6 = current_time()

    log.warning(f"GDP per capita (ms) ............... {t6 - t5}");

    # Post offices
    log.warning("***** Post offices")
    postOfficeDf = df\
        .withColumn("post_office_1m_inh",
                    F.expr("int(post_offices_ct / pop_2016 * 100000000) / 100"))\
        .withColumn("post_office_100k_km2",
                    F.expr("int(post_offices_ct / area * 10000000) / 100"))\
        .drop(
            "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
            "cars_ct", "moto_ct", "agr_area", "agr_prod", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "pop_brazil")\
        .orderBy(F.col("post_office_1m_inh").desc())

    if mode == Mode.CACHE:
        postOfficeDf = postOfficeDf.cache()
    elif mode == Mode.CHECKPOINT:
        postOfficeDf = postOfficeDf.checkpoint(True)
    elif mode == Mode.CHECKPOINT_NON_EAGER:
        postOfficeDf = postOfficeDf.checkpoint(False)

    log.warning("****  Per 1 million inhabitants")

    postOfficePopDf = postOfficeDf\
        .drop("post_office_100k_km2", "area")\
        .orderBy(F.col("post_office_1m_inh").desc())
    postOfficePopDf.show(5)

    log.warning("****  per 100000 km2")
    postOfficeArea = postOfficeDf\
        .drop("post_office_1m_inh", "pop_2016")\
        .orderBy(F.col("post_office_100k_km2").desc())
    postOfficeArea.show(5)
    t7 = current_time()
    log.warning(f"Post offices (ms) ................. {(t7 - t6)} Mode: {mode}")

    # Cars and motorcycles per 1k habitants
    log.warning("***** Vehicles")
    vehiclesDf = df\
        .withColumn("veh_1k_inh", F.expr("int((cars_ct + moto_ct) / pop_2016 * 100000) / 100"))\
        .drop(
            "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
            "post_offices_ct", "agr_area", "agr_prod", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "area",
            "pop_brazil")\
        .orderBy(F.col("veh_1k_inh").desc())
    vehiclesDf.show(5)
    t8 = current_time()
    log.warning(f"Vehicles (ms) ..................... {t8 - t7}")

    # Cars and motorcycles per 1k habitants
    log.warning("***** Agriculture - usage of land for agriculture")
    agricultureDf = df\
        .withColumn("agr_area_pct",F.expr("int(agr_area / area * 1000) / 10"))\
        .withColumn("area",F.expr("int(area)"))\
        .drop(
            "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
            "post_offices_ct", "moto_ct", "cars_ct", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "wal_mart_ct", "pop_brazil", "agr_prod",
            "pop_2016")\
        .orderBy(F.col("agr_area_pct").desc())
    agricultureDf.show(5);

    t9 = current_time()
    log.warning(f"Agriculture revenue (ms) .......... {(t9 - t8)}");

    t10 = current_time();
    log.warning(f"Total with purification (ms) ......  {t10 - t0}");
    log.warning(f"Total without purification (ms) ... {t10 - t0}");

    return t10 - t0

if __name__ == "__main__":

    log = logging.getLogger(__name__)

    spark = SparkSession\
        .builder\
        .appName("Column addition") \
        .master("local[*]")\
        .getOrCreate()

    spark.sparkContext.setLogLevel('warn')
    spark.sparkContext.setCheckpointDir('/tmp/checkpoints')

    df = spark.read.format("csv")\
        .option("header", True)\
        .option("sep", ";")\
        .option("enforceSchema", True)\
        .option("nullValue", "null")\
        .option("inferSchema", True)\
        .load("./data/brazil/BRAZIL_CITIES.csv")

    df.show(10);
    df.printSchema();

    t0 = process(df, Mode.NO_CACHE_NO_CHECKPOINT);

    # // Create and process the records with cache
    t1 = process(df, Mode.CACHE);

    # // Create and process the records with a checkpoint
    t2 = process(df, Mode.CHECKPOINT);

    # // Create and process the records with a checkpoint
    t3 = process(df, Mode.CHECKPOINT_NON_EAGER);

    log.warning("***** Processing times (excluding purification)");
    log.warning("Without cache ............... {0} ms".format(t0));
    log.warning("With cache .................. {0} ms".format(t1));
    log.warning("With checkpoint ............. {0} ms".format(t2));
    log.warning("With non-eager checkpoint ... {0} ms".format(t3));
