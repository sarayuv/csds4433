import random
from faker import Faker
import os

fake = Faker()
num_customers = 500
num_purchases = 3000

# Create directory
dir_path = 'data'
os.makedirs(dir_path, exist_ok=True)

# File paths for separate datasets
customers_file = os.path.join(dir_path, 'customers.csv')
purchases_file = os.path.join(dir_path, 'purchases.csv')

# Generate Customers Dataset
with open(customers_file, 'w') as f:
    f.write("CustID,Name,Age,Address,Salary\n")
    for i in range(1, num_customers + 1):
        name = fake.name().replace(",", "")
        age = random.randint(18, 100)
        address = fake.address().replace(",", "")
        salary = round(random.uniform(1000, 10000), 2)
        f.write(f"{i},{name},{age},{address},{salary}\n")

# Generate Purchases Dataset
with open(purchases_file, 'w') as f:
    f.write("TransID,CustID,TransTotal,TransNumItems,TransDesc\n")
    for i in range(1, num_purchases + 1):
        cust_id = random.randint(1, num_customers)
        trans_total = round(random.uniform(10, 2000), 2)
        num_items = random.randint(1, 15)
        trans_desc = fake.sentence(nb_words=random.randint(20, 50)).replace(",", "")
        f.write(f"{i},{cust_id},{trans_total},{num_items},{trans_desc}\n")

print(f"Customers dataset saved to {customers_file}")
print(f"Purchases dataset saved to {purchases_file}")
