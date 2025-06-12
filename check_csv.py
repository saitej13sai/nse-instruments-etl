import pandas as pd
for file in ["common_stocks.csv", "only_in_upstox.csv", "only_in_dhan.csv"]:
    df = pd.read_csv(f"C:/Users/saite/Downloads/output/{file}")
    print(f"{file} has {len(df)} list!")