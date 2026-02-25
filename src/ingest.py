from pyspark.sql import SparkSession

def main():

    spark = SparkSession.builder \
        .appName("BeninMobileMoneyIngestion") \
        .getOrCreate()

    print("Reading users dataset...")
    users_df = spark.read.csv(
        "data/raw/users.csv",
        header=True,
        inferSchema=True
    )

    print("Reading transactions dataset...")
    transactions_df = spark.read.csv(
        "data/raw/transactions.csv",
        header=True,
        inferSchema=True
    )

    print("Users Schema:")
    users_df.printSchema()

    print("Transactions Schema:")
    transactions_df.printSchema()

    print("Users count:", users_df.count())
    print("Transactions count:", transactions_df.count())

    users_df.show(5)
    transactions_df.show(5)

    spark.stop()

if __name__ == "__main__":
    main()