import pandas as pd

file_path = "C:/Users/add41/Documents/Data_Engineer/Project/example_data/City_Data.csv"

df = pd.read_csv(file_path)

print(df["date"].iloc[-1])