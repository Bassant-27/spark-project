from pyspark.sql.functions import col


def clean_data(df):
    cleaned_df = df.filter(
        (col("amount") > 0) &
        (col("name").isNotNull())
    )
    cleaned_df = cleaned_df.withColumn(
        "amount_with_tax",
        col("amount") * 1.20
    )
    return cleaned_df
