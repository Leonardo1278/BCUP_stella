import pandas as pd
import sqlite3 as sq 
from sqlalchemy import create_engine
import os 
import json 
import re

### Conexiones con BBDD
# Conexión con BBDD en la nube
usuario = "postgres"
contraseña = "NpLtpHmxO5jS7ZIV"
nombrebbdd = "mso_dev"
host = "mso-dev.ccteiwksjvs8.us-east-2.rds.amazonaws.com"
connection_string = f"postgresql://{usuario}:{contraseña}@{host}/{nombrebbdd}"
conn = create_engine(connection_string)

# Conexión con base de datos local
current_dir = os.getcwd()
sqlite_dir = current_dir + ""
pathconexx = os.path.join(sqlite_dir, "00_ZocDoc&ECWDetails.db")
conexx = sq.connect(pathconexx)


### Se generan todas las variables fijas a usar

# Listados para sacar la información del encounterlog
Conceptos = ['004MeritBW:004 Merit - BW',
             '004MeritPB:004 Merit - PB',
             '002Samuell:002 Samuell'   ,
             '001Bishop:001 Bishop'     ,
             '003Colorado:003 Colorado' ]

concepto_buscado = Conceptos

# Se genran patrones de reconocimiento para la extracción de información as+i como los listados para guardar la información 
pattern_DOB = r'DOB: (\d{2}/\d{2}/\d{4})'
pattern_number = r'\(\d{3}\)\s\d{3}-\d{4}'
pattern_id = r'ZocdocId:\s\d+'
pattern_id_unique = r'\d{8}'
pattern_email = r'\S+@\S+'

nacimientos = []
numeros = []
zocDocID = []
IDs = []
mails = []

# Listado de campos que se quieren extraer de la columna "LOG"
columnas = ['FacilityName','VisitType','NewPatient','Reason','VisitStatus','Notes','ProviderName']


### Definición de funciones
def Obtener_1_dicc(cadena_json,llave):
    """Esta funcion recibe una (campo) para poder extraer la información de la columna tipo Json"""
    try:
        data = json.loads(cadena_json)
        if llave in data:
            return data[llave]
    except json.JSONDecodeError:
        pass
    return None

def Obtener_2_dicc(diccionario):
    """Esta función recibe el Old & New Value para extraer únicamente el NewValue"""
    if isinstance(diccionario, dict) and 'NewValue' in diccionario:
        return diccionario['NewValue']
    return None

def notes_translator(patrones, listas, columna):
    """Esta función recibe como parámetros los patrones reconom¿cimiento, las listas de asignación y 
    el campo a analizar, para poder extraer la información de la columna 'NOTES' """
    try:
        for elemento in resultado[columna]:
            # Maneja filas vacías o nulas
            if pd.isna(elemento) or not elemento.strip():
                for lista in listas:
                    lista.append('SIN NOTES')
                continue

            # Utiliza re.search para encontrar la coincidencia en la cadena
            encontrado = False
            for i, patron in enumerate(patrones):
                match = re.search(patron, elemento)
                if match:
                    listas[i].append(match.group(1) if match.group(1) else match.group(0))
                    encontrado = True
                    break
            
            if not encontrado:
                for lista in listas:
                    lista.append('SIN NOTES')

    except Exception as e:
        print(f'Error de registro: {e}')

def lector_tablas(titulo,conexion,query = False):
    """Esta función recibe un titulo y una query para poder llevar la extracción desde el CLOUD BBDD"""
    if query == False:
        query = queryTablas
    a = pd.read_sql_query(query,con=conexion)
    a.to_sql(titulo,conexx, if_exists='replace', index=False)
    tupla = ({titulo:a})
    tablas = []
    tablas.append(tupla)

### Procesamiento

# Se extrae la información de LOG_ZocDoc
query = """
SELECT * 
FROM encounterlog_created_zocdoc 
"""
a = pd.read_sql_query(query, conn)

# Se manda a llamar a las funciones Obtener_1_dicc y Obtener_2_dicc para extraer la información de logdetails
for columna in columnas:
    a[columna] = a['logdetails'].apply(Obtener_1_dicc,args=(columna,))
    a[columna] = a[columna].apply(Obtener_2_dicc)
a.to_sql('encounterlog_created_zd', con=conexx, index=False, if_exists='replace')

# Se genera tabla con la información de los Facilitys agregados a concepto_buscado
partes = []  # Lista para almacenar los resultados parciales
for elemento in concepto_buscado:
    resultado = a[a['FacilityName'] == elemento]
    partes.append(resultado)
resultado = pd.concat(partes)
resultado.to_sql('encounterlog_created_zd_MERRIT', con=conexx, index=False, if_exists='replace')

# Llama a la función para extraer las fechas de nacimiento de la sección de Notas
notes_translator([pattern_DOB], [nacimientos], 'Notes')
    # Verifica y asigna las fechas de nacimiento y correos electrónicos al DataFrame 'resultado'
if len(nacimientos) == len(resultado):
    resultado['DOB'] = nacimientos
else:
    print('Las longitudes no coinciden. No se puede asignar la lista a la columna DOB o MAIL.')
resultado.to_sql('FacilityName', con=conexx, if_exists='replace', index=False)

# Se ordena la tablas por patientID,modifydate y se renombra a FacilityName
q = """SELECT * FROM FacilityName ORDER BY patientID,modifydate"""
a = pd.read_sql_query(q,con=conexx)
a.to_sql('FacilityName', con=conexx, if_exists='replace', index=False)

# Se eliminan duplicados para conocer el numero de pacientes que generaron todos mis logs 
a.drop_duplicates(subset='patientID', keep='last', inplace=True)
a.to_sql('FacilityName_GROUP', con=conexx, if_exists='replace',index=False)

# Se agrega procesamiento y Se genera información de FacilityNameGROUP
q = """
SELECT patientID, count(patientID) as 'Billed_times',
GROUP_CONCAT(Reason || '// ') AS 'Reason',
GROUP_CONCAT(VisitStatus || '// ') AS 'VisitStatus'
FROM FacilityName
GROUP BY patientID
ORDER BY patientID DESC
"""
resultado1 = pd.read_sql_query(q,con=conexx)
resultado1.drop_duplicates(inplace=True)
resultado1.to_sql('FacilityNameGROUP', con=conexx, if_exists='replace', index=False)

# Se genera información de TablasPOSTGRE, visits_detail and encounters_detail
tituloTablas = 'TablasPOSTGRE'
queryTablas = """
SELECT table_name, string_agg(column_name, ', ') AS column_names
    FROM (
            SELECT t.table_name, c.column_name
            FROM information_schema.tables t
            JOIN information_schema.columns c ON t.table_name = c.table_name
            WHERE t.table_schema = 'public'
            ) AS subquery
    GROUP BY table_name;
    """ 
tituloVisitDetail = 'visits_detail'
queryVisitDetail = f"""SELECT * FROM {tituloVisitDetail}"""
tituloEncounterDetails = 'encounters_detail'
queryEncountersDetails = f'''SELECT * FROM {tituloEncounterDetails}'''
tablas_info = {tituloVisitDetail: queryVisitDetail,
               tituloEncounterDetails: queryEncountersDetails}
for titulo, query in tablas_info.items():
    lector_tablas(titulo,conn,query)

# Se genera información de claims_with_cpt_details
q = """
SELECT * 
FROM claims_with_cpt_details
"""
a = pd.read_sql_query(q,con=conn)
a.to_sql('claims_with_cpt_details', con=conexx, if_exists='replace',index=False)

# Se genera información de claims_without_cpt_details
q = """
SELECT * 
FROM claims_without_cpt_details
"""
a = pd.read_sql_query(q,con=conn)
a.to_sql('claims_without_cpt_details', con=conexx, if_exists='replace',index=False)

# Se genera información de AV_claims_with_cpt_details
q = """
SELECT D."Claim No", 
    D."Encounter ID", 
    D."Patient Acct No"
FROM claims_with_cpt_details D
"""
a = pd.read_sql_query(q,con=conn)
a.drop_duplicates(subset='Encounter ID')
a.to_sql('AV_claims_with_cpt_details', con=conexx, index=False, if_exists='replace')
