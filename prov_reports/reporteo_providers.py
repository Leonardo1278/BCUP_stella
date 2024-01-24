import pandas as pd
import sqlite3 as sq 
from sqlalchemy import create_engine

# Conexión con base de datos local
local_db = sq.connect('reporteo_providers.db')

# Conexión con BBDD en la nube
usuario = "postgres"
contraseña = "NpLtpHmxO5jS7ZIV"
nombrebbdd = "stellamattina_data_mart_prod"
host = "mso-dev.ccteiwksjvs8.us-east-2.rds.amazonaws.com"
connection_string = f"postgresql://{usuario}:{contraseña}@{host}/{nombrebbdd}"
cloud_db = create_engine(connection_string)

## Se genera el procesamiento para la tabla de fact_claims
queryfClaims = """SELECT * 
FROM 
    fact_claims C
LEFT JOIN
    dim_appointment_date_calendar_week R
ON 
    C.service_date_calendar_week_id = R.appointment_date_calendar_week_id
LEFT JOIN 
    dim_facility_group F
USING (facility_group_id)
LEFT JOIN 
    dim_primary_insurance P 
USING (primary_insurance_id)
LEFT JOIN dim_resource_provider M
USING (resource_provider_id)
LEFT JOIN dim_visit_type V
USING (visit_type_id);
"""
#dataClaims = pd.read_sql_query(queryfClaims, con=cloud_db)
#dataClaims.to_sql('fact_claims', con=local_db, if_exists= 'replace', index=False)   

claims_filtrados = """SELECT * FROM fact_claims WHERE facility_group_name LIKE "00%" AND is_billable = 1 AND appointment_date_calendar_year = 'Y23'"""
#dataClaimsFiltrados = pd.read_sql_query(claims_filtrados, con=local_db)
#dataClaimsFiltrados.dropna(subset=('primary_insurance_class','visit_type'), inplace=True)
#dataClaimsFiltrados.to_sql('dataClaimsFiltrados', con=local_db, if_exists= 'replace', index=False)  

query_agrupacion = """
SELECT 
    resource_provider_name AS 'PROVIDER',
    SUM(is_billable) as 'VISITS', 
    ROUND(SUM(payment), 0) as 'REVENUE',
    appointment_date_calendar_period as 'PERIOD',
    appointment_date_calendar_year as 'YEAR',
    appointment_date_calendar_week as 'WEEK',
    complexity_level,
    primary_insurance_class,
    visit_type as 'VT',
    SUM(new_patient) AS 'NP'
FROM dataClaimsFiltrados 
GROUP BY resource_provider_name, PERIOD,WEEK ,complexity_level, primary_insurance_class, VT
ORDER BY PERIOD 
"""
data_agrupacion = pd.read_sql_query(query_agrupacion, local_db)
data_agrupacion['Location'] = "CLINIC"
data_agrupacion.to_sql('agrupacion_providers', con=local_db, if_exists='replace', index=False)  



## Se genera el procesamiento para la tabla de fact_cpts (HOSPITAL)

queryCPT = """
SELECT 
    C.*, 
    R.appointment_date_calendar_period,
    R.appointment_date_calendar_week,
    R.appointment_date_calendar_year,
    V.visit_type,
    V.visit_type_description, 
    T.cpt_group,
    T.cpt_description, 
    F.facility_group_name, 
    I.primary_insurance_class,
    I.primary_insurance_group,
    I.primary_insurance_name, 
    M.resource_provider_name AS "PROVIDER"
FROM 
    fact_cpts C
LEFT JOIN
    dim_appointment_date_calendar_week R
ON 
    C.service_date_calendar_week_id = R.appointment_date_calendar_week_id
LEFT JOIN dim_visit_type V
    USING (visit_type_id)
LEFT JOIN dim_cpt T
    USING (cpt_id)
LEFT JOIN dim_facility_group F
    USING (facility_group_id)
LEFT JOIN dim_resource_provider M
    ON C.cpt_provider_allocated_id = M.resource_provider_id
LEFT JOIN dim_primary_insurance I
    USING (primary_insurance_id)
WHERE R.appointment_date_calendar_year = 'Y23' ;
""" 
#dataCPT = pd.read_sql_query(queryCPT, con=cloud_db)
#dataCPT.to_sql('fact_cpts', con=local_db, if_exists= 'replace', index=False)


sec_query = """
SELECT * 
FROM 
    fact_cpts 
WHERE 
    is_billable = 1 
    AND facility_group_name NOT LIKE "00%" 
    AND cpt_description NOT LIKE "%Dummy%"
    AND cpt_description NOT LIKE "%CYSTOSCOPY%" 
"""
#dataCPT2 = pd.read_sql_query(sec_query, con=local_db)
#dataCPT2.dropna(subset=('visit_type',''), inplace=True)
#dataCPT2.to_sql('cpts_filtrados', con=local_db, if_exists='replace', index=False)

query_seleccion_2 = """
SELECT 
    PROVIDER,
    SUM(is_billable) AS 'VISITS', 
    ROUND(SUM(payment), 0) as 'REVENUE',
    appointment_date_calendar_period as 'PERIOD',
    appointment_date_calendar_year as 'YEAR',
    appointment_date_calendar_week as 'WEEK',
    complexity_level,
    primary_insurance_class,
    cpt_group AS 'VT',
    SUM(new_patient) AS 'NP'
FROM 
    cpts_filtrados
GROUP BY PROVIDER,PERIOD,WEEK, complexity_level, primary_insurance_class, VT
"""
data_seleccion_2 = pd.read_sql_query(query_seleccion_2, con=local_db)
data_seleccion_2['Location'] = 'HOSPITAL'
data_seleccion_2.to_sql('agrupacion_hopsital', con=local_db, index=False, if_exists='replace')
data_concatenar = (data_agrupacion,data_seleccion_2)

## Concatenamos información de clinica y hospital
data_general = pd.DataFrame(pd.concat(data_concatenar))
data_general = data_general.sort_values(by=['PROVIDER', 'PERIOD'])
data_general.to_sql('general', con=local_db, if_exists='replace', index=False)
data_general.to_csv('reporteo_providers.csv', index=False)



