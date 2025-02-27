import random
from faker import Faker
import os


fake = Faker()
num_customers = 50000
num_purchases = 5000000

dir_path = 'data'
os.makedirs(dir_path, exist_ok=True)
file_name = os.path.join(dir_path, 'customers_and_purchases.csv')

with open(file_name, 'w') as f:
    f.write("Customers\n")
    f.write("CustID,Name,Age,Address,Salary\n")

    # generate the customers data
    for i in range(1, num_customers + 1):
        name = fake.name().replace(",", "")
        age = random.randint(18, 100)
        address = fake.address().replace(",", "")
        salary = round(random.uniform(1000, 10000), 2)

        # write the customer record
        f.write(f"{i},{name},{age},{address},{salary}\n")

    f.write("\nPurchases\n")
    f.write("TransID,CustID,TransTotal,TransNumItems,TransDesc\n")

    # generate the purchases data
    for i in range(1, num_purchases + 1):
        cust_id = random.randint(1, num_customers)
        trans_total = round(random.uniform(10, 2000), 2)
        num_items = random.randint(1, 15)
        trans_desc = fake.sentence(nb_words=random.randint(20, 50)).replace(",", "")  # Random description

        # write the purchase record
        f.write(f"{i},{cust_id},{trans_total},{num_items},{trans_desc}\n")

print(f"Data has been generated and saved to {file_name}")
