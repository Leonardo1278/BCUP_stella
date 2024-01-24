import pandas as pd
import sqlite3 as sq  

conex = sq.connect('Documents.db')

# se importa la información documents - Report studio 
path = "C:\\Users\\LeonardoDanielCuetoC\\Downloads\\Documents\\Documents_Actual_Real.csv" 
""" QUERY: 
SELECT CAST(attachto AS VARCHAR) AS attachto,
CAST(claimRef AS VARCHAR) AS claimRef,
CAST(consentDocEHX AS VARCHAR) AS consentDocEHX,
CAST(customName AS VARCHAR) AS customName,
CAST(delFlag AS VARCHAR) AS delFlag,
CAST(Description AS VARCHAR) AS Description,
CAST(dirpath AS VARCHAR) AS dirpath,
CAST(doc_Type AS VARCHAR) AS doc_Type,
CAST(docID AS VARCHAR) AS docID,
CAST(documentheight AS VARCHAR) AS documentheight,
CAST(documentwidth AS VARCHAR) AS documentwidth,
CAST(drawingid AS VARCHAR) AS drawingid,
CAST(eHXSentStatus AS VARCHAR) AS eHXSentStatus,
CAST(encId AS VARCHAR) AS encId,
CAST(expiresuppress AS VARCHAR) AS expiresuppress,
CAST(expirydate AS VARCHAR) AS expirydate,
CAST(FacilityId AS VARCHAR) AS FacilityId,
CAST(fileformat AS VARCHAR) AS fileformat,
CAST(filename AS VARCHAR) AS filename,
CAST(folderName AS VARCHAR) AS folderName,
CAST(ftpserver AS VARCHAR) AS ftpserver,
CAST(injuryId AS VARCHAR) AS injuryId,
CAST(insuranceid AS VARCHAR) AS insuranceid,
CAST(migrateflag AS VARCHAR) AS migrateflag,
CAST(modifieddatetime AS VARCHAR) AS modifieddatetime,
CAST(oldfilename AS VARCHAR) AS oldfilename,
CAST(pagecount AS VARCHAR) AS pagecount,
CAST(PatientId AS VARCHAR) AS PatientId,
CAST(pnCatId AS VARCHAR) AS pnCatId,
CAST(pnItemId AS VARCHAR) AS pnItemId,
CAST(portalboxUpload AS VARCHAR) AS portalboxUpload,
CAST(Priority AS VARCHAR) AS Priority,
CAST(proxyuid AS VARCHAR) AS proxyuid,
CAST(publishToEHX AS VARCHAR) AS publishToEHX,
CAST(refId AS VARCHAR) AS refId,
CAST(refReqId AS VARCHAR) AS refReqId,
CAST(refType AS VARCHAR) AS refType,
CAST(resolution AS VARCHAR) AS resolution,
CAST(Review AS VARCHAR) AS Review,
CAST(ReviewerId AS VARCHAR) AS ReviewerId,
CAST(reviewername AS VARCHAR) AS reviewername,
CAST(scanbyid AS VARCHAR) AS scanbyid,
CAST(scanDate AS VARCHAR) AS scanDate,
CAST(ScannedBy AS VARCHAR) AS ScannedBy,
CAST(servicedate AS VARCHAR) AS servicedate,
CAST(sizeinbytes AS VARCHAR) AS sizeinbytes,
CAST(source AS VARCHAR) AS source,
CAST(suggestedpatients AS VARCHAR) AS suggestedpatients,
CAST(TempDocId AS VARCHAR) AS TempDocId,
CAST(tocId AS VARCHAR) AS tocId,
CAST(uniqueDocId AS VARCHAR) AS uniqueDocId,
CAST(wcReportTypeCode AS VARCHAR) AS wcReportTypeCode,
CAST(websync AS VARCHAR) AS websync
FROM document"""

# Se importa la info del reporte de Documents ECW
pathReporte = 'C:\\Users\\LeonardoDanielCuetoC\\Downloads\\Documents\\45.02 - Documents Detail Report (3)_Actual.csv'
""" REPORTE DE ECW, RUTA: 
    Public Folders > eCWEBO > 4 - Adinistrative Reports > 45 - Jelly Bean Reports > 45.2 - Documents Detail Report
    El reporte debe sacarse con rango de tiempo definido, si se desacarga completo genera problemas por el tamaño del archivo, 
    y la lectura directa tiene problemas con los tipos de datos, una vez descargado el reporte se debe copiar la información y pegarla en un archivo 
    csv nuevo, con únicamente formato de números y valores, lo mismo se tiene que hacer con la tabla que sale de la query anterior (document) 
    y aparentemente todo reporte que sale de ECW
"""

# Lectura de datos y primer carga a BBDD - Documents
data = pd.read_csv(path)
data.to_sql('Documents', if_exists='replace',con=conex, index=False)
q = """
SELECT * 
FROM 
    Documents 
WHERE 
    scanDate like '%2023%' 
AND 
    scanDate LIKE '%Nov%'
"""
data = pd.read_sql_query(q,con=conex)
data.to_sql('Documents', if_exists='replace',con=conex, index=False)

# Lectura de datos y primer carga a BBDD - ReporteDocuments
dataReports = pd.read_csv(pathReporte, encoding = 'latin -1')
dataReports[['patientID', 'PatientName']] = dataReports["Patient Acct No + Name"].str.split(':', expand=True)
dataReports.to_sql('ReporteDocuments', if_exists='replace',con=conex, index=False)

# se genera lista con los Doc_Types que deben cosiderarse para la revisión de Documents
doc_types = [1,
             2,
             3,
             4,
             21,
             100,
             101,
             107,
             111,
             115,
             116]

# Se genera listado de todos los distintos ReviewerID que son Providers
reviewers_ids = [9178,
                 9179,
                 9180,
                 207453,
                 208135,
                 209278,
                 249070,
                 122,
                 9176,
                 9177,
                 107939,
                 209277,
                 210751,
                 214661,
                 214662,
                 215147,
                 248321,
                 248327,
                 248415,
                 249070,
                 249985,
                 250381,
                 250717,
                 251037]

# Se filtra la información de documents que está asignada a alguno de los providers 
q = f"""
SELECT * 
FROM 
    Documents 
WHERE 
    ReviewerId in {tuple(reviewers_ids)}
    AND delFlag = 0
"""
a = pd.read_sql_query(q,con=conex)
    # Convertir la columna de fechas a un formato datetime
a['scanDate'] = pd.to_datetime(a['scanDate'], format='%b %d %Y %I:%M%p')
    # Formatear la columna de fechas en el nuevo formato deseado
a['scanDate2'] = a['scanDate'].dt.strftime('%d/%m/%Y')
a.drop_duplicates(subset=('docID'), inplace=True)
a.to_sql('Providers', con=conex, if_exists='replace', index=False)

# Se genera primer cruce de datos entre el Reporte de ECW y la data de Documents 
q = """
SELECT 
    P.'ReviewerId', 
    P.'reviewername', 
    P.'ScannedBy', 
    P.'customName', 
    P.'delFlag', 
    P.'docID', 
    P.'patientId',
    P.'fileformat', 
    P.'filename',
    P.'Review', 
    P.'scanDate2',
    R.'Scanned By' AS 'R_Scanned By', 
    R.'Reviewed Date' AS 'R_Reviewed Date',
    R.'Document ID' AS 'R_Document ID',
    R.'patientID' AS 'R_patientID',
    R.'Reviewer Name' AS 'R_Reviewer Name'
FROM 
    Providers P 
LEFT JOIN 
    ReporteDocuments R 
ON 
    R.'Document ID' = P.'docID' 
"""
a = pd.read_sql_query(q,con=conex)
a.drop_duplicates(subset=('ReviewerId','docID'),inplace=True)
a['R_Reviewed Date'] = pd.to_datetime(a['R_Reviewed Date'], format='%d/%m/%Y')
a['scanDate2'] = pd.to_datetime(a['scanDate2'], format='%d/%m/%Y')
a['DemoraRevision'] = (a['R_Reviewed Date'] - a['scanDate2']).dt.days
a.to_sql('Prov_JOIN_Report', con=conex, if_exists='replace', index=False)

# Se agrega columna de validacion a la tabla del join, validación <= 5 días
q = """
SELECT *,
    CASE
        WHEN 
            DemoraRevision <= 5
        THEN 
            1
        ELSE 
            0
        END
        AS VALIDADOR
FROM 
    Prov_JOIN_Report"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('Prov_JOIN_Report', con=conex, if_exists='replace', index=False)

# Se genera agrupación para empezar a observar las métricas de reelevancia 
q = """
SELECT 
    P.'reviewername', 
    COUNT(*) AS DOCUMENTS,
    SUM(CASE WHEN Review = 1 THEN 1 ELSE 0 END) AS REVIEW,
    SUM(CASE WHEN Review = 0 THEN 1 ELSE 0 END) AS NO_REVIEW,
    SUM(CASE WHEN VALIDADOR LIKE '%1%' THEN 1 ELSE 0 END) AS '<5 DIAS',
    -- Usar NULLIF para evitar la división por cero
    (SUM(CASE WHEN VALIDADOR LIKE '%1%' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN Review = 1 THEN 1 ELSE 0 END), 0)) * 100 AS PERCENTAGE
FROM 
    Prov_JOIN_Report P
WHERE 
    delFlag = 0
GROUP BY 
    P.'ReviewerId'
ORDER BY 
    NO_REVIEW DESC;

"""
a = pd.read_sql_query(q,con=conex)
a.to_sql('GROUP_TIME', con=conex, if_exists='replace', index=False)
a.to_excel('GROUP_TIME.xlsx', index=False)

# Se genera tabla con toda la información que no ha sido revisada
q = """
SELECT  
    ReviewerId, 
    reviewername, 
    ScannedBy, 
    customName, 
    delFlag, 
    docID, 
    fileformat, 
    filename, 
    patientId, 
    Review
FROM 
    Providers 
WHERE 
    Review = 0 
    AND delFlag = 0 
"""
a = pd.read_sql_query(q,con=conex)
a.to_excel('No_Reviewed.xlsx', index=False)
