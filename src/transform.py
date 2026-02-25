from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, desc , round
from pyspark.sql.functions import month, year

def main():

    spark = SparkSession.builder \
        .appName("BeninMobileMoneyTransformation") \
        .getOrCreate()
    # Lecture des données
    transactions_df = spark.read.csv(
        "data/raw/transactions.csv",
        header=True,
        inferSchema=True
    )
    users_df = spark.read.csv(
        "data/raw/users.csv",
        header=True,
        inferSchema=True
    )

    #Jointure des données
    joined_df = transactions_df.join(
        users_df,
        "user_id")

        # Volume total par region
    region_volume = joined_df.groupBy("region") \
    .agg(round(sum("amount"), 2).alias("total_volume")) \
    .orderBy(desc("total_volume"))
    


    region_volume.show()


    ## Top 10 users by transaction volume
    top_users = joined_df.groupBy("user_id") \
    .agg(round(sum("amount"), 2).alias("total_volume")) \
    .orderBy(desc("total_volume")) \
    .limit(10)

    top_users.show()


    ## 

    monthly_volume = joined_df \
    .withColumn("month", month("transaction_date")) \
    .groupBy("month") \
    .agg(round(sum("amount"), 2).alias("total_volume")) \
    .orderBy("month")

    monthly_volume.show()


    ## Enregistrer les resultats transformés en parquet
    region_volume.write.mode("overwrite").parquet("data/processed/region_volume.parquet")
    top_users.write.mode("overwrite").parquet("data/processed/top_users.parquet")
    monthly_volume.write.mode("overwrite").parquet("data/processed/monthly_volume.parquet")

    spark.stop()

if __name__ == "__main__":
    main()