import os, json, click
from pyspark.sql import SparkSession
from .transform import clean_and_enrich
from .quality import null_counts, duplicate_count, summary

@click.group()
def cli():
    """CLI para ejecutar el pipeline PySpark local."""
    pass

@cli.command("run")
@click.option("--input", "input_csv", required=True,
              type=click.Path(exists=True, dir_okay=False), help="CSV de entrada.")
@click.option("--outdir", required=True, type=click.Path(file_okay=False),
              help="Directorio de salida.")
def run(input_csv: str, outdir: str):
    os.makedirs(outdir, exist_ok=True)
    spark = (SparkSession.builder
             .appName("pyspark-taxi-profiler")
             .master("local[*]")
             .getOrCreate())

    df = spark.read.option("header", True).csv(input_csv)
    df_clean = clean_and_enrich(df)

    # Métricas de calidad
    nc = null_counts(df_clean)
    dups = duplicate_count(df_clean, subset=[
        "pickup_datetime","dropoff_datetime","trip_distance_km","fare_amount_eur"
    ])
    summ = summary(df_clean)
    print("=== DATA QUALITY ===")
    print(json.dumps({"nulls": nc, "duplicates": dups, "summary": summ}, indent=2))

    # Convertimos a Pandas y guardamos
    out_csv = os.path.join(outdir, "data", "cleaned.csv")

    pdf = df_clean.toPandas()
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    pdf.to_csv(out_csv, index=False)

    print(f"OK ✓ CSV escrito en: {out_csv}")

    spark.stop()

def main():
    cli()

if __name__ == "__main__":
    main()
