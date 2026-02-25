from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum
from pyspark.sql.functions import current_timestamp

def main():

    spark = SparkSession.builder \
        .appName("BeninMobileMoneyValidation") \
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

     # =========================
    # 1. Vérification des NULLS
    # =========================

    print("===== NULL VALUES IN USERS =====")
    users_df.select([
        sum(col(c).isNull().cast("int")).alias(c) 
        for c in users_df.columns
    ]).show()



    print("===== NULL VALUES IN TRANSACTIONS =====")
    transactions_df.select([
        sum(col(c).isNull().cast("int")).alias(c) 
        for c in transactions_df.columns
    ]).show()




    print("===== NEGATIVE AMOUNTS IN TRANSACTIONS =====")
    negative_count = transactions_df.filter(col("amount") < 0).count()
    print(f"Negative transactions: {negative_count}")

    print("===== ZERO AMOUNTS IN TRANSACTIONS =====")
    zero_count = transactions_df.filter(col("amount") == 0).count()
    print(f"Zero amount transactions: {zero_count}")

    print("===== FUTURE TRANSACTIONS =====")
    future_count = transactions_df.filter(
        col("transaction_date") > current_timestamp()
    ).count()
    print(f"Future transactions: {future_count}")



    ## Dectection de transactions orphelines (sans utilisateur correspondant)
    print("===== ORPHAN TRANSACTIONS =====")
    orphan_count = transactions_df.join(
        users_df,
        transactions_df.user_id == users_df.user_id,
        "left_anti"
    ).count()
    print(f"Orphan transactions: {orphan_count}")

    transactions_df.describe().show()
    transactions_df.groupBy("transaction_type").count().show()
        

    spark.stop()

if __name__ == "__main__":
    main()