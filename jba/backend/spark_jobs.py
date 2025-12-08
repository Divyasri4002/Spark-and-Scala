import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

class JobMarketAnalyzer:
    def __init__(self):
        # Delay creating the Spark session until data is needed so we can
        # validate input files first and avoid JVM startup failures.
        self.spark = None
        
    def load_data(self, file_path):
        """Load and preprocess the job market data"""
        # Resolve relative paths to absolute path based on this file's directory
        if not os.path.isabs(file_path):
            base_dir = os.path.dirname(__file__)
            file_path_abs = os.path.join(base_dir, file_path)
        else:
            file_path_abs = file_path

        # Check existence before creating Spark (gives clearer error to user)
        if not os.path.exists(file_path_abs):
            raise FileNotFoundError(
                f"Dataset file not found at '{file_path_abs}'.\n"
                f"Please place your CSV at this path or update `Config.DATA_PATH` in backend/config.py."
            )

        # Initialize Spark session lazily (now that the file exists)
        if self.spark is None:
            self.spark = SparkSession.builder \
                .appName("JobMarketAnalysis") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")

        df = self.spark.read.option("header", "true").csv(file_path_abs)
        
        # Data cleaning and transformation
        df = df.withColumn("salary_min", col("salary_min").cast("integer"))
        df = df.withColumn("salary_max", col("salary_max").cast("integer"))
        df = df.withColumn("avg_salary", (col("salary_min") + col("salary_max")) / 2)
        df = df.withColumn("posted_date", to_date(col("posteddate"), "yyyy-MM-dd"))
        
        return df
    
    def get_domain_analysis(self, df):
        """Analyze job trends by domain"""
        return df.groupBy("domain") \
            .agg(
                count("*").alias("job_count"),
                avg("avg_salary").alias("avg_salary"),
                avg("salary_min").alias("min_salary"),
                avg("salary_max").alias("max_salary")
            ) \
            .orderBy(desc("job_count")) \
            .toPandas().to_dict('records')
    
    def get_skill_demand(self, df):
        """Analyze most demanded skills"""
        # Explode skills array and count frequency
        skills_df = df.withColumn("skill", explode(split(col("skills"), ", ")))
        
        return skills_df.groupBy("skill") \
            .agg(count("*").alias("demand_count")) \
            .orderBy(desc("demand_count")) \
            .limit(20) \
            .toPandas().to_dict('records')
    
    def get_company_analysis(self, df):
        """Analyze hiring trends by company"""
        return df.groupBy("company") \
            .agg(
                count("*").alias("job_count"),
                avg("avg_salary").alias("avg_salary")
            ) \
            .orderBy(desc("job_count")) \
            .limit(15) \
            .toPandas().to_dict('records')
    
    def get_location_analysis(self, df):
        """Analyze job distribution by location"""
        return df.groupBy("city") \
            .agg(
                count("*").alias("job_count"),
                avg("avg_salary").alias("avg_salary")
            ) \
            .orderBy(desc("job_count")) \
            .toPandas().to_dict('records')
    
    def get_experience_trends(self, df):
        """Analyze salary trends by experience level"""
        return df.groupBy("experience_level") \
            .agg(
                count("*").alias("job_count"),
                avg("avg_salary").alias("avg_salary"),
                avg("salary_min").alias("min_salary"),
                avg("salary_max").alias("max_salary")
            ) \
            .orderBy(desc("job_count")) \
            .toPandas().to_dict('records')
    
    def get_salary_distribution(self, df):
        """Get salary distribution data"""
        return df.select("avg_salary") \
            .filter(col("avg_salary").isNotNull()) \
            .toPandas().to_dict('records')
    
    def stop_spark(self):
        """Stop Spark session"""
        self.spark.stop()