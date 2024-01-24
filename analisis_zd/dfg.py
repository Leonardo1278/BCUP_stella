import pandas as pd 
import sqlite3 as sq
from sqlalchemy import create_engine

usuario = "postgres"
contraseña = "NpLtpHmxO5jS7ZIV"
nombrebbdd = "mso_dev"
host = "mso-dev.ccteiwksjvs8.us-east-2.rds.amazonaws.com"
connection_string = f"postgresql://{usuario}:{contraseña}@{host}/{nombrebbdd}"
conn = create_engine(connection_string)


query = """SELECT * FROM claims_without_cpt_details"""
con = sq.connect('mtp.db')
df = pd.read_sql(query, conn)
df.to_sql('tmp', con=con, if_exists='replace', index=False)