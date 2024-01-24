import pandas as pd
import sqlite3 as sq 
import os 

# Conexión con base de datos local
current_dir = os.getcwd()
sqlite_dir = current_dir + ""
pathconex = os.path.join(sqlite_dir, "00_ZocDoc&ECWDetails.db")
conex = sq.connect(pathconex)

conex2 = sq.connect('tmp.db')

# Se genera información de New Patients
q = """
SELECT patientID 
FROM Transaccionalidad_usuarios_CHK_Validador
WHERE Validador != 0
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('NP', con=conex2, if_exists='replace', index=False)


q = """
SELECT F.*, N.patientID AS PT
FROM NP  N
LEFT JOIN  FacilityName F 
ON N.patientID = F.'patientID'
ORDER BY patientID, modifydate
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('NP_NEW', con=conex2, if_exists='replace', index=False)


a.drop_duplicates(subset='patientID', keep='last', inplace=True)
a.to_sql('NP_NEW', con=conex2, if_exists='replace', index=False)


# Se genera el conteo por type de la tabla FACILITY
q = """
SELECT F.Reason, 
       COUNT(F.Reason) AS ZocDoc
FROM FacilityName_GROUP F
GROUP BY F.Reason; 
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('NP_NEW_group_FA', con=conex2, if_exists='replace', index=False)
#Se verifica la informacion 
q = """SELECT SUM(ZocDoc) from NP_NEW_group_FA"""
print(f'FacilityVerif:\n{pd.read_sql_query(q,con=conex2)}')


# Se genera el conteo por type de la tabla NewPatients
q = """
SELECT Reason, COUNT(*) AS New_PF
FROM NP_NEW 
GROUP BY Reason; 
"""
a = pd.read_sql_query(q,con=conex2)
a.to_sql('NP_NEW_group_NP_', con=conex2, if_exists='replace', index=False)
#Se verifica la informacion 
q = """SELECT SUM(New_PF) from NP_NEW_group_NP_"""
print(f'NewVerif:\n{pd.read_sql_query(q,con=conex2)}')


# Se genera el conteo por type de la tabla NewPatients  y facility
q = """
SELECT
    N.Reason,
    N.ZocDoc,
    P.New_PF
FROM NP_NEW_group_FA N
LEFT JOIN NP_NEW_group_NP_ P
ON N.Reason = P.Reason
GROUP BY N.Reason
ORDER BY N.ZocDoc DESC
"""
a = pd.read_sql_query(q,con=conex2)
a.fillna(value=0, inplace=True)
a.to_sql('NP_NewPatientFinal', con=conex2, if_exists='replace', index=False)    
a.to_excel('NewPatientReasons.xlsx',index=False)

q = """SELECT SUM(ZocDoc) as ZD, SUM(New_PF) AS NP
FROM NP_NewPatientFinal"""
print(f"Verificacion:\n{pd.read_sql_query(q,con=conex2)}")

