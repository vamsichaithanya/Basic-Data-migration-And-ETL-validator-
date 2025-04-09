import pandas as pd
import random  
from concurrent.futures import ThreadPoolExecutor

num_rows = 400000
columns = ["Name", "Email", "Telephone", "City", "Country", "Gender", "Date Of Birth"]
df = pd.read_csv(r"",low_memory=False)


num_rows_to_update = int(num_rows * 0.05)
rows_to_update = random.sample(range(num_rows), num_rows_to_update)
columns_to_update = columns[:-1]  

def update_row(index):
    """ Function to update two random cells in a given row. """
    cols = random.sample(columns_to_update, 2)
    for col in cols:
        if col == "Name":
            df.at[index, col] = f"Updated_{index}"
        elif col == "Email":
            df.at[index, col] = f"updated{index}@example.com"
        elif col == "Telephone":
            df.at[index, col] = f"9876{random.randint(100000,999999)}"
        elif col == "City":
            df.at[index, col] = random.choice(["Sydney", "Dubai", "Toronto", "Madrid", "Rome"])
        elif col == "Country":
            df.at[index, col] = random.choice(["Australia", "UAE", "Canada", "Spain", "Italy"])
        elif col == "Gender":
            df.at[index, col] = random.choice(["M", "F"])


with ThreadPoolExecutor() as executor:
    executor.map(update_row, rows_to_update)


df.to_csv(r"", index=False)

print("Dataset updated successfully!")
