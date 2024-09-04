from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, count, lit, from_json, to_date, year, month, dayofmonth,
    concat_ws, when, length, regexp_replace, split, avg, sum, window, max, min
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.window import Window

schema = StructType([
    StructField("reference_sheet", StringType(), True),
    StructField("version", StringType(), True),
    StructField("legal_nature", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("brand_name", StringType(), True),
    StructField("product_models", StringType(), True),
    StructField("risk_description", StringType(), True),
    StructField("consumer_recommendations", StringType(), True),
    StructField("compensation_methods", StringType(), True),
    StructField("additional_information", StringType(), True),
    StructField("date_of_publication", TimestampType(), True),
    StructField("distributors", StringType(), True),
    StructField("image_url", StringType(), True)
])


def transform_data(df):
    transformed_df = df \
        .withColumn("publication_date", to_date(col("date_of_publication"))) \
        .withColumn("year", year(col("publication_date"))) \
        .withColumn("month", month(col("publication_date"))) \
        .withColumn("day", dayofmonth(col("publication_date"))) \
        .withColumn("cleaned_legal_nature", regexp_replace(col("legal_nature"), r"[^\w\s]", "")) \
        .withColumn("risk_length", length(col("risk_description"))) \
        .withColumn("combined_risk", concat_ws(" - ", col("risk_description"), col("consumer_recommendations"))) \
        .withColumn("high_risk_flag", when(col("risk_length") > 100, lit(1)).otherwise(lit(0))) \
        .withColumn("category_hierarchy", concat_ws(" > ", col("product_category"), col("sub_category"))) \
        .withColumn("brand_count", count(col("brand_name")).over(Window.partitionBy("brand_name"))) \
        .withColumn("is_compensation_provided",
                    when(length(col("compensation_methods")) > 0, lit("Yes")).otherwise(lit("No"))) \
        .withColumn("distributor_list", split(col("distributors"), ",")) \
        .withColumn("num_distributors", expr("size(distributor_list)"))

    filtered_df = transformed_df \
        .filter(col("year") >= 2023)

    filtered_df = filtered_df.withColumn(
        "risk_level",
        when(col("risk_description").rlike("serious|severe|critical"), "High")
        .when(col("risk_description").rlike("moderate|medium"), "Medium")
        .otherwise("Low")
    )

    aggregation_df = filtered_df.groupBy("product_category", "month") \
        .agg(
        count("reference_sheet").alias("total_recalls"),
        sum("high_risk_flag").alias("total_high_risks"),
        count(when(col("is_compensation_provided") == "Yes", True)).alias("total_compensations"),
        avg("risk_length").alias("average_risk_length"),
        max("num_distributors").alias("max_distributors"),
        min("num_distributors").alias("min_distributors")
    ) \
        .orderBy("month")

    trend_df = filtered_df.groupBy(window(col("publication_date"), "1 month"), "product_category") \
        .agg(
        count("reference_sheet").alias("monthly_recalls"),
        sum("high_risk_flag").alias("monthly_high_risks")
    )

    result_df = aggregation_df.join(
        trend_df,
        on=["product_category", "month"],
        how="left"
    )

    return result_df


def main():
    spark = SparkSession.builder \
        .appName("RappelConso") \
        .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
        .getOrCreate()

    subscription = "projects/rappelconso-434222/locations/us-central1/subscriptions/rappelconso-lite-subscription"

    df = spark \
        .readStream \
        .format("pubsublite") \
        .option("pubsublite.subscription", subscription) \
        .load() \
        .select(from_json(col("data").cast("string"), schema).alias("record")) \
        .select("record.*")

    transformed_df = transform_data(df)

    query = transformed_df \
        .writeStream \
        .format("bigquery") \
        .option("table", "your_project.your_dataset.your_table") \
        .option("checkpointLocation", "gs://your-bucket/checkpoints/") \
        .option("writeMethod", "direct") \
        .outputMode("append") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
