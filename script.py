import pandas as pd
import sqlite3 as sq 
from sqlalchemy import create_engine


### Conexión con BBDD en la nube
usuario = "postgres"
contraseña = "NpLtpHmxO5jS7ZIV"
nombrebbdd = "mso_dev"
host = "mso-dev.ccteiwksjvs8.us-east-2.rds.amazonaws.com"
connection_string = f"postgresql://{usuario}:{contraseña}@{host}/{nombrebbdd}"
conn = create_engine(connection_string)

conex = sq.connect('ANS.db')

### Extracción de data 
queryVISITS = """SELECT * FROM visits_detail"""
queryAPPLIED = """SELECT * FROM applied_revenue_v1"""
query_CLAIMS_WITH = """SELECT * FROM claims_with_cpt_details"""
query_CLAIMS_WITHOUT = """SELECT * FROM claims_without_cpt_details"""
QUERY_calendar = """SELECT * FROM fiscal_calendar_2"""
querys = [queryVISITS,queryAPPLIED ,query_CLAIMS_WITH ,query_CLAIMS_WITHOUT,QUERY_calendar]

titulos = ['visits_detail','apllied_rev','claims_with','claims_without','fiscal_calendar']

for query,titulo in zip(querys,titulos): 
    #a = pd.read_sql_query(query,con=conn)
    #a.to_sql(f'{titulo}', con=conex, if_exists='replace', index=False)
    pass

# Se genera la informacion de visits unida a fiscal calendar 
query_vis_cal = """
SELECT * 
FROM 
    visits_detail v 
LEFT JOIN 
    fiscal_calendar f  
ON 
    v.'Appointment_Date' = f.'dates' 
WHERE 
    Visit_Status LIKE '%Check Out%'"""
#a = pd.read_sql_query(query_vis_cal,con=conex)
#a.to_sql(f'visits_detail2', con=conex, if_exists='replace', index=False)

# se gnera la ifnormacion de app revenue unida a fiscal calendar 
query_vis_cal = """SELECT * FROM apllied_rev v LEFT JOIN fiscal_calendar f  ON v.'Service_Date' = f.'dates'"""
#a = pd.read_sql_query(query_vis_cal,con=conex)
#a.to_sql(f'apllied_REV_REV', con=conex, if_exists='replace', index=False)

# Se genera la información de app collections unida a fiscal calendar 
query_vis_cal = """SELECT * FROM apllied_rev v LEFT JOIN fiscal_calendar f  ON v.'Payment_Deposit_Date' = f.'dates'"""
#a = pd.read_sql_query(query_vis_cal,con=conex)
#a.to_sql(f'apllied_col', con=conex, if_exists='replace', index=False)

# Se genera la ifnormación de calims unida a fiscal calendar 
query_vis_cal = """SELECT * FROM claims_without v LEFT JOIN fiscal_calendar f  ON v.'Service Date' = f.'dates'"""
#a = pd.read_sql_query(query_vis_cal,con=conex)
#a.to_sql(f'claims_without', con=conex, if_exists='replace', index=False)


# Se genera la ifnormación de calims unida a fiscal calendar 
query_vis_cal = """SELECT * FROM claims_with v LEFT JOIN fiscal_calendar f  ON v.'Service Date' = f.'dates'"""
#a = pd.read_sql_query(query_vis_cal,con=conex)
#a.to_sql(f'claims_with', con=conex, if_exists='replace', index=False)


### PROCESAMIENTO DE LAS TABLAS CLAIMS

periodos = ['FY23 - 7', 'FY23 - 8', 'FY23 - 9', 'FY23 - 10', 'FY23 - 11']

# Se genera información de Claims Without proveniente de los periodos
queryclaim = f"""SELECT * FROM claims_without 
WHERE 
    period IN {tuple(periodos)}"""
a = pd.read_sql_query(queryclaim, con=conex)
a.to_sql('claims_without_p11', con=conex, if_exists='replace', index=False)

# Se genera información de Claims With proveniente de los periodos
queryclaim = f"""SELECT * FROM claims_with
WHERE 
    period IN {tuple(periodos)} """
a = pd.read_sql_query(queryclaim, con = conex)
a.to_sql(f'claims_with_p11', con=conex, if_exists='replace', index=False)

# Se genra agrupación por Payer
q = """SELECT `Resource Provider Name`,period,`Payer Class` ,COUNT(DISTINCT `Claim No`)
FROM claims_with_p11
GROUP BY `Resource Provider Name`,period, `Payer Class`
ORDER BY `Resource Provider Name`,period ;
"""
a = pd.read_sql_query(q, con = conex)
a.to_sql(f'claims_with_p11_PAYER', con=conex, if_exists='replace', index=False)
a.to_excel('claims_with_p11_PAYER.xlsx', index=False)

# Se genera agrupación por Quality 
q = """SELECT `Resource Provider Name`,period,`Quality Level` ,COUNT(DISTINCT `Claim No`)
FROM claims_with_p11
GROUP BY `Resource Provider Name`,period ,`Quality Level`
ORDER BY `Resource Provider Name`,period ;
"""
a = pd.read_sql_query(q, con = conex)
a.to_sql(f'claims_with_p11_QUALITY', con=conex, if_exists='replace', index=False)
a.to_excel('claims_with_p11_QUALITY.xlsx', index=False)

# Se genera agrupación por Visit Type
q = """SELECT `Resource Provider Name`,period,`Visit Type Classification` ,COUNT(DISTINCT `Claim No`)
FROM claims_without_p11
GROUP BY `Resource Provider Name`,period ,`Visit Type Classification`
ORDER BY `Resource Provider Name`,period ;
"""
a = pd.read_sql_query(q, con = conex)
a.to_sql(f'claims_without_p11_Vtype', con=conex, if_exists='replace', index=False)
a.to_excel('claims_without_p11_Vtype.xlsx', index=False)

q = """SELECT * FROM claims_with"""
a = pd.read_sql_query(q,con=conex)
a.drop_duplicates(subset=['Claim No'], keep = 'first', inplace=True)
a.to_sql('claims_with_NDuplicated', con=conex, if_exists='replace')


### PROCESAMIENTO DE LAS TABLAS REV - REVENUE 
q = f"""
SELECT 
    c.'Resource Provider Name', 
    a.period, 
    c.'Payer Class',
    COALESCE(SUM(a.Payment), 0) AS TotalPayment
FROM 
    apllied_rev_rev a
LEFT JOIN 
    claims_with_NDuplicated c
ON 
    a.'Claim_No' = c.'Claim No' 
WHERE 
    a.period IN {tuple(periodos)}
GROUP BY 
    c.'Resource Provider Name', a.period, c.'Payer Class'
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('appW_Resource_PAYER', con=conex, if_exists='replace', index=False)
a.to_excel('appW_Resource_PAYER.xlsx', index=False)


q = f"""
SELECT 
    a.'Encounter_ID', c.'Encounter ID'
FROM 
    apllied_rev_rev a
LEFT JOIN 
    claims_with_NDuplicated c
ON 
    a.'Claim_No' = c.'Claim No' 
WHERE 
    a.period IN {tuple(periodos)}

"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('appW_PAYER', con=conex, if_exists='replace', index=False)


q = f"""
SELECT c.'Resource Provider Name', a.period,c.'Quality Level', COALESCE(SUM(a.Payment), 0) AS TotalPayment
FROM 
    apllied_rev_rev a
LEFT JOIN claims_without c
ON a.'Claim_No' = c.'Claim No' 
WHERE a.period IN {tuple(periodos)}
GROUP BY c.'Resource Provider Name', a.period,c.'Quality Level'
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('appW_Resource_QUALITY', con=conex, if_exists='replace', index=False)
a.to_excel('appW_Resource_QUALITY.xlsx', index=False)

q = f"""
SELECT c.'Resource Provider Name', a.period,c.'Visit Type Classification' ,COALESCE(SUM(a.Payment), 0) AS TotalPayment
FROM 
    apllied_rev_rev a
LEFT JOIN claims_without c
ON a.'Claim_No' = c.'Claim No' 
WHERE a.period IN {tuple(periodos)}
GROUP BY c.'Resource Provider Name', a.period, c.'Visit Type Classification'
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('appW_Resource_Vtype', con=conex, if_exists='replace', index=False)
a.to_excel('appW_Resource_Vtype.xlsx', index=False)

### PROCESAMIENTO DE LAS TABLAS REV - COLLECTIONS 
q = f"""
SELECT 
    c.'Resource Provider Name', 
    a.period, 
    c.'Payer Class',
    COALESCE(SUM(a.Payment), 0) AS TotalPayment
FROM 
    apllied_col a
LEFT JOIN 
    claims_with_NDuplicated c
ON 
    a.'Claim_No' = c.'Claim No' 
WHERE 
    a.period IN {tuple(periodos)}
GROUP BY 
    c.'Resource Provider Name', a.period, c.'Payer Class'
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('appCOL_Resource_PAYER', con=conex, if_exists='replace', index=False)
a.to_excel('appCOL_Resource_PAYER.xlsx', index=False)

q = f"""
SELECT c.'Resource Provider Name', a.period,c.'Quality Level', COALESCE(SUM(a.Payment), 0) AS TotalPayment
FROM 
    apllied_col a
LEFT JOIN claims_without c
ON a.'Claim_No' = c.'Claim No' 
WHERE a.period IN {tuple(periodos)}
GROUP BY c.'Resource Provider Name', a.period,c.'Quality Level'
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('appCOL_Resource_QUALITY', con=conex, if_exists='replace', index=False)
a.to_excel('appCOL_Resource_QUALITY.xlsx', index=False)

q = f"""
SELECT c.'Resource Provider Name', a.period,c.'Visit Type Classification' ,COALESCE(SUM(a.Payment), 0) AS TotalPayment
FROM 
    apllied_col a
LEFT JOIN claims_without c
ON a.'Claim_No' = c.'Claim No' 
WHERE a.period IN {tuple(periodos)}
GROUP BY c.'Resource Provider Name', a.period, c.'Visit Type Classification'
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('appCOL_Resource_Vtype', con=conex, if_exists='replace', index=False)
a.to_excel('appCOL_Resource_Vtype.xlsx', index=False)



