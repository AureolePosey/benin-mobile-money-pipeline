import random
import csv
from datetime import datetime, timedelta
import os

def main():

        ## Verifier si le dossier existe sinon le créer
        os.makedirs("data/raw", exist_ok=True)

        NUM_USERS = 5000

        regions = ["Cotonou", "Porto-Novo", "Parakou", "Abomey", "Bohicon"]
        operators = ["MTN", "Moov", "Celtiis"]

        users_path = "data/raw/users.csv"

        with open(users_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["user_id", "region", "operator", "registration_date"])
            
            for i in range(1, NUM_USERS + 1):
                registration_date = datetime.now() - timedelta(days=random.randint(0, 365))
                writer.writerow([
                    i, 
                    random.choice(regions), 
                    random.choice(operators), 
                    registration_date.strftime("%Y-%m-%d")])


        print("Les données des utilisateurs ont été générées dans le fichier avec succes:")


        NUM_TRANSACTIONS = 50000
        transactions_types = ["deposit", "withdraw", "transfer"]
        transactions_path = "data/raw/transactions.csv"


        with open(transactions_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                "transaction_id",
            "user_id", 
            "transaction_type",
            "amount", 
            "transaction_date"])
            
            for i in range(1, NUM_TRANSACTIONS + 1):
                timestamp = datetime.now() - timedelta(days=random.randint(0, 365))
                writer.writerow([ 
                    i, 
                    random.randint(1, NUM_USERS), 
                    random.choice(transactions_types), 
                    round(random.uniform(500, 50000), 2), 
                    timestamp.strftime("%Y-%m-%d %H:%M:%S")])
if __name__ == "__main__":
    main()


print("Les données des transactions ont été générées dans le fichier avec succes:")