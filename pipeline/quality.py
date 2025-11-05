from pyspark.sql import DataFrame

def null_counts(df: DataFrame) -> dict:
    return {c: df.filter(df[c].isNull()).count() for c in df.columns}

def duplicate_count(df: DataFrame, subset: list[str]) -> int:
    total = df.count()
    distinct = df.dropDuplicates(subset).count()
    return total - distinct

def summary(df: DataFrame) -> dict:
    return {"rows": df.count(), "cols": len(df.columns)}
