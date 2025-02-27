import pandas as pd
import numpy as np

np.random.seed(42)

num_people = 1500000

# Generate unique IDs
person_ids = [f"p{i}" for i in range(1, num_people + 1)]

# Generate names (random strings for simplicity)
names = [f"Person_{i}" for i in range(1, num_people + 1)]

# Generate table assignments (e.g., tables 1-100)
table_assignments = np.random.randint(1, 101, size=num_people)

# Generate flu test results (sick or not-sick)
# Assume 5% of people are sick
flu_test_results = np.random.choice(["sick", "not-sick"], size=num_people, p=[0.05, 0.95])

# Create the Meta-Event dataset
meta_event_data = {
    "id": person_ids,
    "name": names,
    "table": table_assignments,
    "test": flu_test_results
}
meta_event_df = pd.DataFrame(meta_event_data)

# Create the Meta-Event-No-Disclosure dataset
# This dataset excludes the 'test' column
meta_event_no_disclosure_df = meta_event_df.drop(columns=["test"])

# Create the Reported-Illnesses dataset
# This dataset contains only people who tested sick
reported_illnesses_df = meta_event_df[meta_event_df["test"] == "sick"][["id", "test"]]

# Save datasets to files
meta_event_df.to_csv("Meta-Event.csv", index=False)
meta_event_no_disclosure_df.to_csv("Meta-Event-No-Disclosure.csv", index=False)
reported_illnesses_df.to_csv("Reported-Illnesses.csv", index=False)

print("Datasets created successfully!")
