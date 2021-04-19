"""
Load raw data from stat.customs.ru in dbf files to one sqlite file

1. Unzip
2. Read dbf to pandas DataFrame
3. Convert char to double
4. Write to sql
"""

import shutil
import os
import glob
from simpledbf import Dbf5
import sqlite3
import pandas as pd

all_zip_files = glob.glob("data/customs_ru_raw/*.zip")
# file = all_zip_files[0]

for file in all_zip_files:
    # Unzip
    shutil.unpack_archive(file, "data/temp", "zip")
    new_file_name = "data/temp/" +\
                    file.removeprefix("data/customs_ru_raw\\").removesuffix(".zip") +\
                    ".dbf"
    os.rename("data/temp/DATTSVT.dbf", new_file_name)

    # Read dbf to DataFrame
    df = Dbf5(new_file_name, codec="CP866").to_dataframe()
    # Rename columns
    df = df.rename(columns={"Stoim": "usd",
                       "Netto": "kg",
                       "Kol": "qty",
                       "period": "year_month",
                       "edizm": "uom",
                       "Region": "region",
                       "Region_s": "federal_district"}
              )

    # Convert char to float
    df["usd"] = df["usd"].str.replace(",", ".").astype(float)
    df["kg"] = df["kg"].str.replace(",", ".").astype(float)
    df["qty"] = df["qty"].str.replace(",", ".").astype(float)
    # Convert string to date
    df["year_month"] = pd.to_datetime(df["year_month"], dayfirst=True, format="%m/%Y")

    # Write to sqlite
    # Create connection
    con = sqlite3.connect("data/impex.sqlite")
    df.to_sql(name="impex", con=con,  if_exists="append")

con.close()
