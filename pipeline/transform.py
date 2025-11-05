from pyspark.sql import DataFrame, functions as F

def clean_and_enrich(df: DataFrame) -> DataFrame:
    typed = (df
        .withColumn("pickup_datetime", F.to_timestamp("pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("trip_distance_km", F.col("trip_distance_km").cast("double"))
        .withColumn("fare_amount_eur", F.col("fare_amount_eur").cast("double"))
    )

    filtered = (typed
        .filter((F.col("trip_distance_km") >= 0) & (F.col("trip_distance_km") <= 200))
        .filter((F.col("fare_amount_eur") >= 0) & (F.col("fare_amount_eur") <= 1000))
    )

    dedup = filtered.dropDuplicates([
        "pickup_datetime","dropoff_datetime","trip_distance_km","fare_amount_eur"
    ])

    enriched = (dedup
        .withColumn("year", F.year("pickup_datetime"))
        .withColumn("month", F.month("pickup_datetime"))
        .withColumn("trip_minutes",
                    (F.col("dropoff_datetime").cast("long")
                     - F.col("pickup_datetime").cast("long")) / 60.0)
    )
    return enriched
