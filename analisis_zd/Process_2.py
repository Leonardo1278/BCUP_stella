import pandas as pd
import sqlite3 as sq 
import os 
import re
import numpy as np
from datetime import datetime

# Conexión con base de datos local
current_dir = os.getcwd()
sqlite_dir = current_dir + ""
pathconex = os.path.join(sqlite_dir, "00_ZocDoc&ECWDetails.db")
conex = sq.connect(pathconex)

# se procesa y estandariza la informacion que viene directamente de la factudración de ZocDoc
path = 'C:\\Users\\LeonardoDanielCuetoC\\OneDrive - Stella Mattina\\Desktop\\Análisis ZocDoc\\appointmentreport_6.csv'
data = pd.read_csv(path)
def convertidor_fechas(columna):
    fechas_estandarizadas = []

    for fecha_original in columna:
        if re.search(r'\b[APap][Mm]\b', fecha_original):
            # Si la fecha incluye AM/PM, se asume formato de 12 horas
            try:
                fecha_obj = datetime.strptime(fecha_original, "%m/%d/%Y %I:%M %p")
                fecha_estandarizada = fecha_obj.strftime("%m/%d/%Y")
            except ValueError:
                fecha_estandarizada = "Fecha no válida"
        else:
            # Si no hay AM/PM, se asume formato de 24 horas
            try:
                fecha_obj = datetime.strptime(fecha_original, "%m/%d/%Y %H:%M")
                fecha_estandarizada = fecha_obj.strftime("%m/%d/%Y")
            except ValueError:
                # Si no se proporciona hora, simplemente se conserva la fecha
                try:
                    fecha_obj = datetime.strptime(fecha_original, "%m/%d/%Y")
                    fecha_estandarizada = fecha_obj.strftime("%m/%d/%Y")
                except ValueError:
                    fecha_estandarizada = "Fecha no válida"
        fechas_estandarizadas.append(fecha_estandarizada)

    return fechas_estandarizadas

fechas_DOB = convertidor_fechas(data['PatientDateOfBirth'])
fechas_App_Time = convertidor_fechas(data['AppointmentTime'])
fechas_Book_time = convertidor_fechas(data['BookingTime'])

data['PatientDateOfBirth'] = fechas_DOB
data['AppointmentTime'] = fechas_App_Time
data['BookingTime'] = fechas_Book_time
data.to_sql('Facturacion_ZocDoc', con=conex, if_exists='replace', index=False)

# Se genera archivo Facturacion_ZocDoc_with_fecha
q = """
SELECT * FROM Facturacion_ZocDoc
where BookingTime <= '12/01/2023'
"""
a = pd.read_sql_query(q,con=conex)
a['dia'] = ''
a['mes'] = ''
a.to_sql('Facturacion_ZocDoc_with_fecha', con=conex, if_exists='replace', index=False)

# Se generan las columnas de identificacion (dia - semana- mes)
query = "SELECT BookingTime FROM Facturacion_ZocDoc_with_fecha"
b = pd.read_sql_query(query,con=conex)
for fila in b.iterrows():
    fecha_str = fila[1]['BookingTime']  # Obtiene el valor de la columna 'BookingTime'
    fecha_obj = datetime.strptime(fecha_str, "%m/%d/%Y")
    # Extraer el día de la fecha
    dia = fecha_obj.day
    mes = fecha_obj.month
    # Actualizar la columna 'dia' con el valor del día para la fila actual
    update_query = "UPDATE Facturacion_ZocDoc_with_fecha SET dia = ? WHERE BookingTime = ?"
    update_query2 = "UPDATE Facturacion_ZocDoc_with_fecha SET mes = ? WHERE BookingTime = ?"
    conex.execute(update_query, (dia, fecha_str))
    conex.execute(update_query2, (mes, fecha_str))
# Guarda los cambios en la base de datos
conex.commit()
querySemanas = """ SELECT * FROM Facturacion_ZocDoc_with_fecha """
Facturacion_ZocDoc_with_fecha = pd.read_sql_query(querySemanas, con=conex)
def asignar_semana(dia):
    dia = int(dia)
    if dia <= 7:
        return 'S1'
    elif dia <= 15:
        return 'S2'
    elif dia <= 23:
        return 'S3'
    else:
        return 'S4'
Facturacion_ZocDoc_with_fecha['semana'] = Facturacion_ZocDoc_with_fecha['dia'].apply(asignar_semana)

# Se manda tabla Facturacion_ZocDoc_with_fecha a BBDD
Facturacion_ZocDoc_with_fecha.to_sql('Facturacion_ZocDoc_with_fecha', con=conex, if_exists='replace', index=False)

# Se mandan a imprimir Transacciones cobradas por ZocDoc
q = """SELECT *
            FROM Facturacion_ZocDoc_with_fecha
            WHERE Premium = 'Yes'"""
dataBillingUnica = pd.read_sql_query(q,con=conex)
print(f'\n# Transacciones cobradas por ZocDoc: ',len(dataBillingUnica))

#Se mandan a imprimir datos de Users cobrados por ZocDoc
q = """SELECT *, GROUP_CONCAT(PatientFirstName || ' ' || PatientLastName) as 'Name'
            FROM Facturacion_ZocDoc_with_fecha
            WHERE Premium = 'Yes'
            GROUP BY PatientEmail"""
dataBillingUnica = pd.read_sql_query(q,con=conex)
dataBillingUnica.to_sql('Facturacion_ZocDoc_with_fecha_BILLED', con=conex, if_exists='replace', index=False)
print(f'\n# Users cobrados por ZocDoc: ',len(dataBillingUnica))

# Se manda a imprimir Facturacion_ZocDoc_with_fecha_byWEEK
q = """
SELECT semana, COUNT(semana) as Times 
FROM Facturacion_ZocDoc_with_fecha_BILLED 
group by semana
"""
Facturacion_ZocDoc_with_fecha_byWEEK = pd.read_sql_query(q,con=conex)
print('\n',Facturacion_ZocDoc_with_fecha_byWEEK)





#Se hace un cruce de la información de LogZocDoc y Encounters Detail
queryZocDetail = """SELECT l.*, e.'Encounter_ID', e.'Visit_Status', e.'Appointment_Date' as 'DateEncounter',e.'Appointment_Provider_Name'
FROM FacilityName l
LEFT JOIN encounters_detail e
on e.'Encounter_ID' = l.'encid'"""
dataZocDetail = pd.read_sql_query(queryZocDetail, con=conex)
dataZocDetail.to_sql('FacturacionZocDoc_join_EncountersDetail', con=conex, if_exists='replace', index=False)
print(f'\nNum transacciones:', len(dataZocDetail))
print(f'\nNum Usuarios:', len(dataZocDetail.drop_duplicates(subset='patientID')))

# se genera información de FacturacionZocDoc_join_EncountersDetail_notNULL
queryZocDetail = """SELECT * FROM FacturacionZocDoc_join_EncountersDetail 
                        where Encounter_ID is not null 
                        order by patientID,appointment_date"""
dataZocDetail = pd.read_sql_query(queryZocDetail, con=conex)
dataZocDetail.to_sql('FacturacionZocDoc_join_EncountersDetail_notNULL', con=conex, if_exists='replace', index=False)

# Se genera información de FacturacionZocDoc_join_EncountersDetail_notNULL_byUSER
queryZocDetail_user = """
SELECT *, GROUP_CONCAT(patientID), COUNT(patientID)
FROM FacturacionZocDoc_join_EncountersDetail_notNULL
WHERE Visit_Status = 'CHK : Check Out'
GROUP BY patientID
"""
dataZocDetail_user = pd.read_sql_query(queryZocDetail_user, con=conex)
dataZocDetail_user.drop_duplicates(subset='patientID', inplace=True)
dataZocDetail_user.to_sql('FacturacionZocDoc_join_EncountersDetail_notNULL_byUSER', con=conex, if_exists='replace', index=False)

# Se genera información de FacturacionZocDoc_join_EncountersDetail_notNULL_Details
q1 = """WITH cte AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY patientID,DOB ORDER BY appointment_date) AS rn
  FROM FacturacionZocDoc_join_EncountersDetail_notNULL
)
SELECT *,
       CASE WHEN Visit_Status = 'CHK : Check Out' THEN 
            ROW_NUMBER() OVER (PARTITION BY patientID,DOB ORDER BY rn)
       ELSE NULL 
       END AS 'WHERE_CHK',
       CASE WHEN Visit_Status =  'CHK : Check Out' THEN 1
       ELSE 0 
       END AS 'IF_CHK',
       SUM(CASE WHEN Visit_Status = 'CHK : Check Out' THEN 1 ELSE 0 END) OVER (PARTITION BY patientID,DOB ORDER BY rn) AS 'CHK TIMES'
FROM cte
ORDER BY patientID,appointment_date, "CHK TIMES"
"""
dataConteo = pd.read_sql_query(q1, con=conex)
dataConteo.to_sql('FacturacionZocDoc_join_EncountersDetail_notNULL_Details', con=conex, if_exists='replace', index=False)

# Se genera información de ConteoCHK
q = """SELECT * FROM FacturacionZocDoc_join_EncountersDetail_notNULL_Details WHERE Visit_Status = 'CHK : Check Out'"""
dataConteoCHK = pd.read_sql_query(q, con=conex)
del dataConteoCHK['IF_CHK']
#dataConteoCHK.drop_duplicates(subset='patientID', keep='first')
repetido = dataConteoCHK['patientID'].duplicated(keep='last')
dataConteoCHK['CHK TIMES'] = np.where(repetido, '', dataConteoCHK['CHK TIMES'])
dataConteoCHK['CHK TIMES'] = dataConteoCHK['CHK TIMES'].replace('', np.nan)
dataConteoCHK['CHK TIMES'] = dataConteoCHK['CHK TIMES'].astype(float).astype(pd.Int32Dtype())
#dataConteoCHK.to_sql('ConteoCHK', con = conex, if_exists='replace', index=False)
repetido1 = dataConteoCHK['patientID'].duplicated(keep='first')
dataConteoCHK['WHERE_CHK'] = np.where(repetido1, '', dataConteoCHK['WHERE_CHK'])
dataConteoCHK['WHERE_CHK'] = dataConteoCHK['WHERE_CHK'].replace('', np.nan)
dataConteoCHK['WHERE_CHK'] = dataConteoCHK['WHERE_CHK'].astype(float).astype(pd.Int32Dtype())
dataConteoCHK.to_sql('ConteoCHK', con = conex, if_exists='replace', index=False)

# Se genera información de ConteoCHK_not_duplicated
dataConteoCHKLAST = dataConteoCHK.drop_duplicates(subset=['patientID'], keep= 'first')
dataConteoCHKLAST.to_sql('ConteoCHK_not_duplicated', con = conex, if_exists='replace', index=False)

# Se manda a imprimir la mezcla de appointments
q = """
SELECT
  SUM(CASE WHEN WHERE_CHK = 1 THEN 1 ELSE 0 END) AS cantidad_1,
  SUM(CASE WHEN WHERE_CHK = 2 THEN 1 ELSE 0 END) AS cantidad_2,
  SUM(CASE WHEN WHERE_CHK = 3 THEN 1 ELSE 0 END) AS cantidad_3,
  SUM(CASE WHEN WHERE_CHK = 4 THEN 1 ELSE 0 END) AS cantidad_4
FROM conteoCHK;"""
a = pd.read_sql_query(q,con=conex)

# Se manda a imprimir la cantidad total de CHK
a['cantidad_mayor'] = a['cantidad_2'] + a['cantidad_3'] + a['cantidad_4']
print(f'\nMezcla de appointments: \n{a}')
print(f'\nFirst CHK: \n',a['cantidad_1'].values,'\nNot First CHK:\n',a['cantidad_mayor'].values, '\nMove to: ',len(dataConteoCHK))

# Se manda a imprimir num de Unicos Usuarios  con First CHK
q = """SELECT *, GROUP_CONCAT(patientID)
        FROM conteoCHK
         WHERE WHERE_CHK = 1
          GROUP BY patientID """
a = pd.read_sql_query(q,con=conex)
print(f'\nUnicos Usuarios  con First CHK: ', len(a))

# Se manda a imprimir num de Unicos Usuarios con CHK
q = """SELECT *, 
        GROUP_CONCAT(DISTINCT Encounter_ID) as 'ECID'
        FROM conteoCHK
        GROUP BY patientID """
dataUnicos_Usuarios_CHK = pd.read_sql_query(q,con=conex)
dataUnicos_Usuarios_CHK.to_sql('Unicos_Usuarios_CHK',con=conex,index=False, if_exists='replace')
print(f'\nUnicos Usuarios con CHK: ', len(dataUnicos_Usuarios_CHK))

# se imprime el promedio de veces que un user tarda en ser CHK
q = """
SELECT avg(WHERE_CHK) as 'AVG'
FROM ConteoCHK
"""
a = pd.read_sql_query( q,con=conex)
print(f'\npromedio de veces que un user tarda en ser CHK: \n{a.values}')

# Listado total de usuarios 
q = """
SELECT patientID, appointment_date, encid
FROM encounterlog_created_zd
order by appointment_date """
a = pd.read_sql_query(q,con=conex)
a.drop_duplicates(subset='patientID', inplace=True, keep='first')
a.to_sql('Users', con=conex, if_exists='replace', index=False)

# Usuarios que generaron un CHK
q ="""
SELECT C.*, 
U.appointment_date as 'first v sinc zoc' 
FROM ConteoCHK_not_duplicated C 
left join Users U on C.'patientID' = U.'patientID'
"""
a = pd.read_sql_query(q,con=conex)
a.drop_duplicates(subset='patientID', inplace=True)
a.to_sql('ConteoCHK_not_duplicated', con=conex, if_exists='replace', index=False)

# Se genera tabla con toda la transaccionalidad de usuarios CHK
q = """
SELECT V.*,
C.encid, 
C.patientID, 
C.FacilityName, 
C.VisitType, 
C.Reason AS 'Reason CHK', 
C.VisitStatus,
C."first v sinc zoc", 
C.appointment_date AS "AP_D"
FROM  ConteoCHK_not_duplicated C
LEFT JOIN visits_detail V
ON C.patientID = V.Patient_Acct_No
WHERE  V.Appointment_Date >= C."first v sinc zoc"
AND V.Visit_Status = 'CHK : Check Out'
ORDER BY C.patientID
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('Transaccionalidad_usuarios_CHK', con=conex, if_exists='replace', index=False)
print(f'\nTotal Check Out:\n{len(a)}')

# Se genera tabla con los unicos usuarios de la transaccionalidad de usuarios CHK
q = """
SELECT 
    SUM(T.'NP_Flag_From_Claim') as Validador, 
    T.'patientID', T."first v sinc zoc", 
    GROUP_CONCAT(DISTINCT T.'Visit_Type') as Reason
FROM 
    Transaccionalidad_usuarios_CHK T
GROUP BY 
    patientID
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('Transaccionalidad_usuarios_CHK_Validador', con=conex, if_exists='replace', index=False)
print(f'\nTotal Check Out(Users):\n{len(a)}')

# Se genra información de los costos de estos usuarios 
q = """
SELECT N.*, C.'Claim No', T.'Payment', T.'Billed Charge' 
FROM Transaccionalidad_usuarios_CHK N
LEFT JOIN Claims_and_encounters C
ON C.'Encounter ID' = N.'Encounter_ID'
LEFT JOIN claims_without_cpt_details T
ON T.'Claim No' = C.'Claim No'
WHERE  T.'Service Date' >= N."first v sinc zoc"
"""
a = pd.read_sql_query(q,con=conex)
a.drop_duplicates( inplace=True)
a.to_sql('ALLPatientBilling', con=conex, if_exists='replace', index=False)
print(f'\nALLPatientBillingCases:\n{len(a)}')

# SE SACA MONTO COBRADO POR ESOS USUARIOS 
q = """
SELECT SUM (Payment) AS TotalPayment 
from ALLPatientBilling
"""
a = pd.read_sql_query(q,con=conex)
print(f'\nALLPatientBillingPayment:\n{a}')

# Sacamos las visitas de NEWPATIENTS
q = """
SELECT C.*, T.'first v sinc zoc' AS 'FECHA INICIAL'
FROM Transaccionalidad_usuarios_CHK  T
LEFT JOIN Transaccionalidad_usuarios_CHK_Validador C
ON T.'patientID' = C.'patientID'
WHERE C.Validador = 1"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('NewPatients', con=conex, if_exists='replace', index=False)

# Sacamos las visitas de ESTABLISHEDPATIENTS
q = """
SELECT C.*, T.'first v sinc zoc' as 'FECHA INCIAL'
FROM Transaccionalidad_usuarios_CHK  T
LEFT JOIN Transaccionalidad_usuarios_CHK_Validador C
ON T.'patientID' = C.'patientID'
WHERE C.Validador = 0"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('EstablishedPatients', con=conex, if_exists='replace', index=False)

# Se manda a borrar la tabla NewPatientsBILLING por que el script presentaba temas
conex.execute("DROP TABLE IF EXISTS NewPatientsBILLING")

# Se genera información de NewPatientsBILLING
q = """
SELECT A.*
FROM ALLPatientBilling A
LEFT JOIN Transaccionalidad_usuarios_CHK_Validador T
ON A.'Patient_Acct_No' = T.'patientID'
WHERE T.'Validador' = 1
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('NewPatientsBILLING', con=conex,  if_exists='replace' , index=False)

# Se genera información de EstablishedPatientsBILLING
q = """
SELECT A.*
FROM ALLPatientBilling A
LEFT JOIN Transaccionalidad_usuarios_CHK_Validador T
ON A.'Patient_Acct_No' = T.'patientID'
WHERE T.'Validador' = 0
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('EstablishedPatientsBILLING', con=conex , if_exists='replace', index=False)

# se manda a imprimir el monto cobrado por los NewPatients
q = """
SELECT SUM(Payment) as Payment
FROM NewPatientsBILLING
"""
a = pd.read_sql_query(q,con=conex)
print(f'\nMonto por NewPatient: \n{a}')

# Se manda a imprimir el monto cobrado por lo EstablishedPatients
q = """
SELECT SUM(Payment) as Payment
FROM EstablishedPatientsBILLING
"""
a = pd.read_sql_query(q,con=conex)
print(f'\nMonto por EstablishedPatients: \n{a}')

