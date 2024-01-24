import pandas as pd
import sqlite3 as sq 
import psycopg2

usuario = "postgres"
contraseña = "NpLtpHmxO5jS7ZIV"
nombrebbdd = "stellamattina_data_mart_prod"
host = "mso-dev.ccteiwksjvs8.us-east-2.rds.amazonaws.com"

# Crear la cadena de conexión
connection_string = f"dbname={nombrebbdd} user={usuario} password={contraseña} host={host}"

# Realizar la conexión
connt = psycopg2.connect(connection_string)

# Generar db local
local_deb = sq.connect('noshow.db')

status_dicc = {'noShow':5,
               }

##Se importa información de visits_Detail
q = """
SELECT 
    F.appointment_date, 
    F.facility_group_id,
    F.patient_id, 
    F.is_new_patient,
    F.general_visit_status_id,
    F.new_patient_flag_from_claim,
    D.facility_group_name
FROM 
    fact_visits_detail F
LEFT JOIN 
    dim_facility_group D
ON F.facility_group_id = D.facility_group_id
WHERE 
    F.appointment_date::text LIKE '%2024-01-0%' 

"""
a = pd.read_sql_query(q,con=connt)
a.to_sql('visits_detail', con=local_deb, if_exists='replace', index=False)

# Se importa la información de dim_primary_insurance
q = """SELECT     
    C.*, 
    P.Primary_insurance_class as PIC 
FROM 
    fact_claims C
LEFT JOIN 
    dim_primary_insurance P  
ON 
    C.primary_insurance_id = P.primary_insurance_id 
ORDER BY 
    C.service_date"""
a = pd.read_sql_query(q,con=connt)
a.to_sql('claims', con=local_deb, if_exists='replace', index=False)


##COMIENZA PROCESAMIENTO

# Se genera listado con unicos usuarios NoShow de visits_detail
q ="""
SELECT 
    patient_id, 
    facility_group_name,
    general_visit_status_id,
    is_new_patient,
    appointment_date
FROM 
    visits_detail
"""
a = pd.read_sql_query(q,con=local_deb)
a.to_sql('users_visits_detail', con=local_deb, if_exists='replace', index=False)

# Se trae la información de claims de los usuarios Noshow del día anterior
q = """
SELECT 
    V.'patient_id' as PID, 
    V.'facility_group_name' as 'FACILITY',
    V.general_visit_status_id,
    V.is_new_patient,
    v.appointment_date,
    C.* 
FROM 
    users_visits_detail V 
LEFT JOIN 
    claims C 
ON 
    V.'patient_id' = C.'patient_id'"""
a = pd.read_sql_query(q,con=local_deb)
a['primary_insurance_id'] = a['primary_insurance_id'].fillna(0).astype(int)
a.to_sql('join',con=local_deb, if_exists='replace', index=False)

q = """SELECT 
(j.PID), 
FACILITY,
MAX(j.primary_insurance_id) AS last_primary_insurance_id,
MAX(j.PIC) AS last_primary_insurance_group,
general_visit_status_id,
is_new_patient,
appointment_date
FROM "join" j
GROUP BY j.PID
ORDER BY FACILITY, last_primary_insurance_group
"""
a = pd.read_sql_query(q,con=local_deb)
a.to_sql('users_join',con=local_deb, if_exists='replace', index=False)

##Comienza segundo procesamiento 

# Se trae la informacion de Samuell y Galloway
a = a[(a['FACILITY'] == '002 Samuell') | (a['FACILITY'] == '005 Galloway')]
a.to_sql('s&m', con=local_deb, if_exists='replace', index=False)    
a.to_csv('S&M.csv', index=False)
