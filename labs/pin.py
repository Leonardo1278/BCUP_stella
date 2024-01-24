
import sqlite3 as sq
import pandas as pd
import datetime as dt 

fechaHOY = dt.datetime.today()

conex = sq.connect('DB\\labs.db')

# Se importa data del arhcivo csv descargado del ReportStrudio
"""
QUERY: 

SELECT CAST(ActualFasting AS VARCHAR) AS ActualFasting,
    CAST(Addendum AS VARCHAR) AS Addendum,
    CAST(approvalstatus AS VARCHAR) AS approvalstatus,
    CAST(assignedToId AS VARCHAR) AS assignedToId,
    CAST(Billable AS VARCHAR) AS Billable,
    CAST(billClient AS VARCHAR) AS billClient,
    CAST(billType AS VARCHAR) AS billType,
    CAST(cancelled AS VARCHAR) AS cancelled,
    CAST(ccresulttolist AS VARCHAR) AS ccresulttolist,
    CAST(ccResultToListIds AS VARCHAR) AS ccResultToListIds,
    CAST(clinicalinfo AS VARCHAR) AS clinicalinfo,
    CAST(collDate AS VARCHAR) AS collDate,
    CAST(collDescription AS VARCHAR) AS collDescription,
    CAST(collSource AS VARCHAR) AS collSource,
    CAST(collTime AS VARCHAR) AS collTime,
    CAST(collUnits AS VARCHAR) AS collUnits,
    CAST(collVolume AS VARCHAR) AS collVolume,
    CAST(commonNotesFlag AS VARCHAR) AS commonNotesFlag,
    CAST(deleteFlag AS VARCHAR) AS deleteFlag,
    CAST(displayIndex AS VARCHAR) AS displayIndex,
    CAST(dtEmailSent AS VARCHAR) AS dtEmailSent,
    CAST(emailSent AS VARCHAR) AS emailSent,
    CAST(EncounterId AS VARCHAR) AS EncounterId,
    CAST(fasting AS VARCHAR) AS fasting,
    CAST(fromLabId AS VARCHAR) AS fromLabId,
    CAST(futureflag AS VARCHAR) AS futureflag,
    CAST(FutureOrderDate AS VARCHAR) AS FutureOrderDate,
    CAST(HasACopy AS VARCHAR) AS HasACopy,
    CAST(hl7id1 AS VARCHAR) AS hl7id1,
    CAST(hl7id2 AS VARCHAR) AS hl7id2,
    CAST(hl7id3 AS VARCHAR) AS hl7id3,
    CAST(HMFlag AS VARCHAR) AS HMFlag,
    CAST(IntNotes AS VARCHAR) AS IntNotes,
    CAST(ItemId AS VARCHAR) AS ItemId,
    CAST(labrefid AS VARCHAR) AS labrefid,
    CAST(newcol AS VARCHAR) AS newcol,
    CAST(Notes AS VARCHAR) AS Notes,
    CAST(OBFLabType AS VARCHAR) AS OBFLabType,
    CAST(ordencounterid AS VARCHAR) AS ordencounterid,
    CAST(OrderInstructions AS VARCHAR) AS OrderInstructions,
    CAST(ordPhyId AS VARCHAR) AS ordPhyId,
    CAST(ordPhyName AS VARCHAR) AS ordPhyName,
    CAST(patientIdentifier AS VARCHAR) AS patientIdentifier,
    CAST(performedby AS VARCHAR) AS performedby,
    CAST(PregId AS VARCHAR) AS PregId,
    CAST(prevencounterid AS VARCHAR) AS prevencounterid,
    CAST(priority AS VARCHAR) AS priority,
    CAST(profileID AS VARCHAR) AS profileID,
    CAST(PSEHold AS VARCHAR) AS PSEHold,
    CAST(publishToPortal AS VARCHAR) AS publishToPortal,
    CAST(reason AS VARCHAR) AS reason,
    CAST(received AS VARCHAR) AS received,
    CAST(refPhyFlag AS VARCHAR) AS refPhyFlag,
    CAST(ReportId AS VARCHAR) AS ReportId,
    CAST(result AS VARCHAR) AS result,
    CAST(ResultDate AS VARCHAR) AS ResultDate,
    CAST(resultime AS VARCHAR) AS resultime,
    CAST(resultSentStatus AS VARCHAR) AS resultSentStatus,
    CAST(ResultStatus AS VARCHAR) AS ResultStatus,
    CAST(ReviewedBy AS VARCHAR) AS ReviewedBy,
    CAST(ReviewedDate AS VARCHAR) AS ReviewedDate,
    CAST(ReviewedTime AS VARCHAR) AS ReviewedTime,
    CAST(SendVmsg AS VARCHAR) AS SendVmsg,
    CAST(status AS VARCHAR) AS status,
    CAST(tolabid AS VARCHAR) AS tolabid,
    CAST(transDate AS VARCHAR) AS transDate,
    CAST(transTime AS VARCHAR) AS transTime,
    CAST(Type AS VARCHAR) AS Type,
    CAST(vmid AS VARCHAR) AS vmid,
    CAST(websync AS VARCHAR) AS websync
    FROM labdata
"""

path = """C:\\Users\\LeonardoDanielCuetoC\\Downloads\\Labs\\labsdata.csv"""
data= pd.read_csv(path, encoding='latin -1')
data.to_sql('LabData', con=conex, index=False, if_exists='replace')

# Se genera diccionario de todos los ID´s que son provenientes de InHouse-Labs
inhouse_orders = {'ClinitekStatus SG': 421754,
                  'Glucose fingerstick': [237286,
                                          529715,
                                          415843],
                  'Hemoglobin fingerstick': 224270,
                  'Pregnancy Test, Urine': [224118,
                                            234614,
                                            386108,
                                            416981,
                                            416982,
                                            429352,
                                            465191,
                                            467771,
                                            475777,
                                            478319,
                                            487756,
                                            521585,
                                            386109,
                                            224119,
                                            429353,
                                            417027],
                  'Urinalysis dipstick': 224082,
                  'Wet Prep':[229075,
                              235939,
                              395857,
                              436989,
                              466517,
                              468810,
                              470981,
                              477774]}

# Se genera listado con todos los distintos ID´s provenientes de los InHouse-Labs
inhouse_orders_CODES = [421754,
                237286,
                529715,
                415843,
                224270,
                224118,
                234614,
                386108,
                416981,
                416982,
                429352,
                465191,
                467771,
                475777,
                478319,
                487756,
                521585,
                386109,
                224119,
                429353,
                417027,
                224082,
                229075,
                235939,
                395857,
                436989,
                466517,
                468810,
                470981,
                477774]

# Se genera filtro para la información de Octubre 2023 - labsData
q = """
SELECT *
FROM 
    LabData
WHERE 
    ResultDate like '%Nov%' 
    and ResultDate like '%2023%'
    
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('labsData', con=conex, if_exists='replace', index=False)
print(f'Total Labs: {len(a)}')
q = """SELECT COUNT (DISTINCT EncounterId) FROM labsData"""
b = pd.read_sql_query(q,con=conex)
print(f'Encounters: {b}')

# Se convierten los distintos campos de Fecha a su valor datetime para poder llevar en análisis 
a['collDate'] = pd.to_datetime(a['collDate'], format='%b %d %Y %I:%M%p')
a['ResultDate'] = pd.to_datetime(a['ResultDate'], format='%b %d %Y %I:%M%p')
a['ReviewedDate'] = pd.to_datetime(a['ReviewedDate'], format='%b %d %Y %I:%M%p')
a['fechaHOY'] = fechaHOY

# Calcular la diferencia entre collDate y ResultDate
a['Difference_CollDate_ResultDate'] = a.apply(lambda row: row['ResultDate'] - row['collDate'] if '1901' not in str(row['ResultDate']) else 'Sin Resultados', axis=1)

# Calcular la diferencia entre ResultDate y ReviewedDate
a['Difference_ResultDate_ReviewedDate'] = a.apply(lambda row: row['ReviewedDate'] - row['ResultDate'] if '1901' not in str(row['ReviewedDate']) else 'Sin revisión', axis=1)

# Calcular la diferencia en días entre ResultDatey ReviewedDate
a['Difference_ResultDate_ReviewedDateDays'] = a['Difference_ResultDate_ReviewedDate'].apply(lambda x: x.days if isinstance(x, pd.Timedelta) else None)

# Convertir las columnas de Timedelta a cadenas de texto
a['Difference_CollDate_ResultDate'] = a['Difference_CollDate_ResultDate'].astype(str)
a['Difference_ResultDate_ReviewedDate'] = a['Difference_ResultDate_ReviewedDate'].astype(str)

# Se manda tabla principal a la bbdd
a.to_sql('labs2', con=conex, if_exists='replace', index=False)

## agregar query para filtrar los out - se deben revisar 
q = f"""SELECT * 
FROM 
    labs 
WHERE 
    Difference_CollDate_ResultDate NOT LIKE '%NaT%'
    AND ItemId IN {tuple(inhouse_orders_CODES)}"""
z = pd.read_sql_query(q,con=conex)
z.to_sql('labs', con=conex, if_exists='replace', index=False)



q = f"""SELECT * FROM labs WHERE 
Difference_CollDate_ResultDate NOT LIKE '%NaT%' """
a = pd.read_sql_query(q,con=conex)
a.to_sql('LABS_CON_RESPUESTA', con=conex, if_exists='replace', index=False)
print(f'Labs con respuesta: {len(a)}')
q = """SELECT COUNT (DISTINCT EncounterId) FROM LABS_CON_RESPUESTA"""
a = pd.read_sql_query(q,con=conex)
print(f'Encounters: {a}')


q = f"""SELECT * FROM labs WHERE 
Difference_CollDate_ResultDate NOT LIKE '%NaT%'
AND ItemId IN {tuple(inhouse_orders_CODES)}"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('LABS_resp_out', con=conex, if_exists='replace', index=False)
print(f'OUT Labs con respuesta: {len(a)}')
q = """SELECT COUNT (DISTINCT EncounterId) FROM LABS_resp_out"""
a = pd.read_sql_query(q,con=conex)
print(f'Encounters: {a}')


q = f"""SELECT * FROM labs WHERE 
Difference_CollDate_ResultDate NOT LIKE '%NaT%'
AND ItemId NOT IN {tuple(inhouse_orders_CODES)}"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('LABS_resp_in', con=conex, if_exists='replace', index=False)
print(f'IN Labs con respuesta: {len(a)}')
q = """SELECT COUNT (DISTINCT EncounterId) FROM LABS_resp_in"""
a = pd.read_sql_query(q,con=conex)
print(f'Encounters: {a}')


q = f"""SELECT * FROM labs WHERE 
Difference_CollDate_ResultDate LIKE '%NaT%' """
a = pd.read_sql_query(q,con=conex)
a.to_sql('LABS_SIN_RESPUESTA', con=conex, if_exists='replace', index=False)
print(f'Labs sin respuesta: {len(a)}')
q = """SELECT COUNT (DISTINCT EncounterId) FROM LABS_SIN_RESPUESTA"""
a = pd.read_sql_query(q,con=conex)
print(f'Encounters: {a}')

q = f"""SELECT * FROM labs WHERE 
Difference_CollDate_ResultDate LIKE '%NaT%'
AND ItemId NOT IN {tuple(inhouse_orders_CODES)}"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('LABS_s_resp_out', con=conex, if_exists='replace', index=False)
print(f'OUT Labs sin respuesta: {len(a)}')
q = """SELECT COUNT (DISTINCT EncounterId) FROM LABS_s_resp_out"""
a = pd.read_sql_query(q,con=conex)
print(f'Encounters: {a}')


q = f"""SELECT * FROM labs WHERE 
Difference_CollDate_ResultDate LIKE '%NaT%'
AND ItemId  IN {tuple(inhouse_orders_CODES)}"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('LABS_s_resp_in', con=conex, if_exists='replace', index=False)
print(f'IN Labs sin respuesta: {len(a)}')
q = """SELECT COUNT (DISTINCT EncounterId) FROM LABS_s_resp_in"""
a = pd.read_sql_query(q,con=conex)
print(f'Encounters: {a}')








# Se genera dato del promedio de demora en revisión de Documentos
q = """
SELECT *
FROM labs
WHERE Difference_CollDate_ResultDate LIKE '0%' AND Difference_ResultDate_ReviewedDate != 'Sin revisión'
"""
b = pd.read_sql_query(q,con=conex)
b.to_sql('LabsINHouse', con=conex, if_exists='replace', index=False)


q = """SELECT *
FROM labs
WHERE NOT (Difference_CollDate_ResultDate LIKE '0%' AND Difference_ResultDate_ReviewedDate != 'Sin revisión');
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('labs1', con=conex, if_exists='replace', index=False)


q = f"""SELECT * FROM labs WHERE ItemId NOT IN {tuple(inhouse_orders_CODES)}"""

a = pd.read_sql_query(q,con=conex)
a.to_sql('PruebasLabs1', con=conex, if_exists='replace', index=False)


q = f"""SELECT * FROM labs WHERE 
Difference_CollDate_ResultDate LIKE '%NaT%' """
a = pd.read_sql_query(q,con=conex)
a.to_sql('labs_s_respuesta', con=conex, if_exists='replace', index=False)
print(f'Labs sin respuesta: {len(a)}')


q = """SELECT * FROM labs where ResultDate NOT LIKE '%1901%' """
a = pd.read_sql_query(q,con=conex)
a.to_sql('LABS_CON_RESPUESTA', con=conex, if_exists='replace', index=False)


# Se genera tabla LabsGroup
q = """
SELECT Difference_ResultDate_ReviewedDateDays, 
    COUNT(*) 
FROM "LABS_CON_RESPUESTA" 
GROUP BY Difference_ResultDate_ReviewedDateDays
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('LasbGroup', con=conex, if_exists='replace', index=False)


# Se genera tabla LabsGroupReviewer
q = """
SELECT ReviewedBy, 
    GROUP_CONCAT(DISTINCT assignedTold) AS ASSIGNEDTO,
    GROUP_CONCAT(DISTINCT ordPhyId || '//') AS ORDERYBY,
    GROUP_CONCAT(DISTINCT ordPhyName || '//') AS ORDERNAME,
    ROUND(AVG(Difference_ResultDate_ReviewedDateDays),2) AS AVG_REV, 
    COUNT(*) AS LABS
FROM LABS_CON_RESPUESTA 
GROUP BY ReviewedBy"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('LasbGroupReviewer', con=conex, if_exists='replace', index=False)
a.to_csv('LabsGroupReviewer.csv', index=False)




# Se genera tabla LabsGroupReviewer
q = """
SELECT 
    ReviewedBy, 
    GROUP_CONCAT(DISTINCT assignedTold) AS ASSIGNEDTO,
    GROUP_CONCAT(DISTINCT ordPhyId) AS ORDERYBY,
    GROUP_CONCAT(DISTINCT ordPhyName) AS ORDERNAME,
    ROUND(AVG(Difference_ResultDate_ReviewedDateDays),2) AS AVG_REV, 
    COUNT(*) AS LABS
FROM 
    "LABS_CON_RESPUESTA" 
WHERE Difference_ResultDate_ReviewedDateDays <= 3
GROUP BY ReviewedBy"""
menor24 = pd.read_sql_query(q,con=conex)
menor24.to_sql('LasbGroupReviewerMENOR24', con=conex, if_exists='replace', index=False)



# Se genera tabla LabsGroupReviewer
q = """
SELECT 
    ReviewedBy, 
    GROUP_CONCAT(DISTINCT assignedTold) AS ASSIGNEDTO,
    GROUP_CONCAT(DISTINCT ordPhyId) AS ORDERYBY,
    GROUP_CONCAT(DISTINCT ordPhyName) AS ORDERNAME,
    ROUND(AVG(Difference_ResultDate_ReviewedDateDays),2) AS AVG_REV, 
    COUNT(*) AS LABS
FROM 
    "LABS_CON_RESPUESTA" 
WHERE Difference_ResultDate_ReviewedDateDays > 3 AND Difference_ResultDate_ReviewedDateDays <= 5
GROUP BY ReviewedBy"""
menor72 = pd.read_sql_query(q,con=conex)
menor72.to_sql('LasbGroupReviewerMENOR72', con=conex, if_exists='replace', index=False)





# Se genera tabla LabsGroupReviewer
q = """
SELECT 
    ReviewedBy, 
    GROUP_CONCAT(DISTINCT assignedTold) AS ASSIGNEDTO,
    GROUP_CONCAT(DISTINCT ordPhyId) AS ORDERYBY,
    GROUP_CONCAT(DISTINCT ordPhyName) AS ORDERNAME,
    ROUND(AVG(Difference_ResultDate_ReviewedDateDays),2) AS AVG_REV, 
    COUNT(*) AS LABS
FROM 
    "LABS_CON_RESPUESTA" 
WHERE Difference_ResultDate_ReviewedDateDays > 5 AND Difference_ResultDate_ReviewedDateDays <= 7
GROUP BY ReviewedBy"""
menor120 = pd.read_sql_query(q,con=conex)
menor120.to_sql('LasbGroupReviewerMENOR120', con=conex, if_exists='replace', index=False)



# Se genera tabla LabsGroupReviewer
q = """
SELECT 
    ReviewedBy, 
    GROUP_CONCAT(DISTINCT assignedTold) AS ASSIGNEDTO,
    GROUP_CONCAT(DISTINCT ordPhyId) AS ORDERYBY,
    GROUP_CONCAT(DISTINCT ordPhyName) AS ORDERNAME,
    ROUND(AVG(Difference_ResultDate_ReviewedDateDays),2) AS AVG_REV, 
    COUNT(*) AS LABS
FROM 
    "LABS_CON_RESPUESTA" 
WHERE Difference_ResultDate_ReviewedDateDays > 7 AND Difference_ResultDate_ReviewedDateDays <= 14
GROUP BY ReviewedBy"""
menor240 = pd.read_sql_query(q,con=conex)
menor240.to_sql('LasbGroupReviewerMENOR240', con=conex, if_exists='replace', index=False)



# Se genera tabla LabsGroupReviewer
q = """
SELECT 
    ReviewedBy, 
    GROUP_CONCAT(DISTINCT assignedTold) AS ASSIGNEDTO,
    GROUP_CONCAT(DISTINCT ordPhyId) AS ORDERYBY,
    GROUP_CONCAT(DISTINCT ordPhyName) AS ORDERNAME,
    ROUND(AVG(Difference_ResultDate_ReviewedDateDays),2) AS AVG_REV, 
    COUNT(*) AS LABS
FROM 
    "LABS_CON_RESPUESTA" 
WHERE Difference_ResultDate_ReviewedDateDays >14
GROUP BY ReviewedBy"""
mayor240 = pd.read_sql_query(q,con=conex)
mayor240.to_sql('LasbGroupReviewerMAYOR240', con=conex, if_exists='replace', index=False)




q = """
SELECT L.*, 
T.LABS AS "<3" ,
M.LABS AS "<5" ,
I.LABS AS "<7" ,
R.LABS AS "<14" ,
I_.LABS AS ">14" ,
ROUND(CAST(M.LABS AS FLOAT) / L.LABS,2) AS "Percentage"
FROM 
    LasbGroupReviewer L 
LEFT JOIN LasbGroupReviewerMENOR24 T 
ON T.'ReviewedBy' = L.'ReviewedBy'
LEFT JOIN LasbGroupReviewerMENOR72 M 
    ON M.'ReviewedBy' = L.'ReviewedBy'
LEFT JOIN LasbGroupReviewerMENOR120 I 
    ON I.'ReviewedBy' = L.'ReviewedBy'
LEFT JOIN LasbGroupReviewerMENOR240 R
    ON R.'ReviewedBy' = L.'ReviewedBy'
LEFT JOIN LasbGroupReviewerMAYOR240 I_ 
    ON I_.'ReviewedBy' = L.'ReviewedBy'
ORDER BY 
    "Percentage" DESC, "AVG_REV" ASC
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('LasbGroupReviewerMENOR', con=conex, if_exists='replace', index=False)






# Se genera tabla LabsGroupReviewer
q = """
SELECT *
FROM "LABS_CON_RESPUESTA" 
WHERE ReviewedBy = 0 or ReviewedBy = '0'"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('Respuesta_sinRevisar', con=conex, if_exists='replace', index=False)

# Se genera tabla LabsGroupReviewer
q = """
SELECT assignedTold, 
    GROUP_CONCAT(DISTINCT ordPhyId) AS ORDERBY,
    ROUND(AVG(Difference_ResultDate_ReviewedDateDays),2) AS AVG_REV, 
    COUNT(*) AS LABS
FROM Respuesta_sinRevisar
GROUP BY assignedTold"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('DetalleS_Revisar', con=conex, if_exists='replace', index=False)
a.to_csv('DetalleS_Revisar.csv', index=False)

# Se genera tabla LabsGroupReviewer
q = """
SELECT assignedTold, 
    GROUP_CONCAT(DISTINCT ordPhyId) AS ORDERBY,
    ROUND(AVG(Difference_ResultDate_ReviewedDateDays),2) AS AVG_REV, 
    COUNT(*) AS LABS
FROM labs_s_respuesta
GROUP BY assignedTold"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('DetalleS_Respuesta', con=conex, if_exists='replace', index=False)
a.to_csv('DetalleS_Respuesta.csv', index=False)


# Se genera LabsGroupENCID
q = """
SELECT 
    EncounterID,
    COUNT(*) AS 'LABS',
    ROUND(AVG(Difference_ResultDate_ReviewedDateDays), 2) AS 'AVG',
    SUM(CASE WHEN Difference_ResultDate_ReviewedDate = 'Sin revisión' THEN 1 ELSE 0 END) AS 'Pendientes'
FROM labs1
GROUP BY EncounterID;
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('LasbGroupENCIDGENERAL', con=conex, if_exists='replace', index=False)

# Se genera LabsGroupENCID
q = """
SELECT 
    EncounterID,
    COUNT(*) AS 'LABS',
    ROUND(AVG(Difference_ResultDate_ReviewedDateDays), 2) AS 'AVG',
    SUM(CASE WHEN Difference_ResultDate_ReviewedDate = 'Sin revisión' THEN 1 ELSE 0 END) AS 'Pendientes'
FROM "LABS_CON_RESPUESTA" 
GROUP BY EncounterID;
"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('LasbGroupENCID', con=conex, if_exists='replace', index=False)


q = f"""SELECT * FROM labs_s_respuesta where ItemId not in {tuple(inhouse_orders_CODES)}"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('Labs_S_respuestaNotInHouseOrders', con=conex, if_exists='replace', index=False)

q = """SELECT COUNT(DISTINCT EncounterId) FROM Labs_S_respuestaNotInHouseOrders"""
a = pd.read_sql_query(q,con=conex)
print(a)


q = f"""SELECT * FROM labs_s_respuesta where ItemId in {tuple(inhouse_orders_CODES)}"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('Labs_respuestaNotInHouseOrders', con=conex, if_exists='replace', index=False)

q = """SELECT EncounterId, COUNT(*) from labs_s_respuesta GROUP BY EncounterId"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('SinRespGROUP', con=conex, if_exists='replace', index=False)

