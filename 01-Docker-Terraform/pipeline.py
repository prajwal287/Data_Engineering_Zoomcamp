import sys
import pandas as pd

print("arguments:", sys.argv)

month = int(sys.argv[1])
df =pd.DataFrame({"num":[1,2,3], "pass":[True, False, True]})
df[month]=month
print(df.head())

df.to_parquet(f"output_{month}.parquet")

print(f"hello pipeline, month={month}")
