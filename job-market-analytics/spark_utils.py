from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, explode, count, avg, to_date, trim
)

def create_spark():
    spark = SparkSession.builder \
        .appName("Job Market Analytics") \
        .master("local[*]") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()
    return spark


def load_data(spark):
    return spark.read.csv("data/raw.csv", header=True, inferSchema=True)


def preprocess(df):
    df = df.withColumn("posteddate", to_date(col("posteddate")))
    df = df.withColumn("skill", explode(split(col("skills"), ",")))
    df = df.withColumn("skill", trim(col("skill")))
    return df


def domain_jobs(df):
    return df.groupBy("domain") \
        .agg(count("*").alias("jobs")) \
        .orderBy(col("jobs").desc())


def top_skills(df, limit=10):
    return df.groupBy("skill") \
        .agg(count("*").alias("demand")) \
        .orderBy(col("demand").desc()) \
        .limit(limit)


def experience_jobs(df):
    return df.groupBy("experience_level") \
        .agg(count("*").alias("jobs"))


def salary_domain(df):
    return df.groupBy("domain") \
        .agg(avg("salary_max").alias("avg_salary")) \
        .orderBy(col("avg_salary").desc())


def job_trend(df):
    return df.groupBy("posteddate") \
        .agg(count("*").alias("jobs")) \
        .orderBy("posteddate")
