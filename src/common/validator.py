import yaml
from pathlib import Path
import pandas as pd

p = Path(f"../data/schemas/daily_sales.yaml")
with open(p) as f:
    y = yaml.safe_load(f)

column_datatypes = {key:value["datatype"] for key, value in y["columns"].items()}



df = pd.read_csv("../data/landing/data.csv", delimiter=";", dtype="str")

df.dt