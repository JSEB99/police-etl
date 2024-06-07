import os
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

# Datos de conexión a la base de datos
db_host = "databasepolices.c1kgoa40yta2.us-east-2.rds.amazonaws.com"
db_name = "databasepolices"
db_user = "etlproyecto"
db_password = "etl"
# Puerto predeterminado de PostgreSQL
db_port = "5432"
# Nombre de la tabla donde se alojaran los datos de manera temporal
table_name = 'OLTP.STAGE_DATA'


def create_table(conn, table_name):

    # Consulta para eliminar la tabla si existe
    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"

    # Consulta para crear la tabla con el nombre especificado
    create_table_query = f'''
    CREATE TABLE {table_name} (
        FECHA TIMESTAMP,
        DEPARTAMENTO VARCHAR(20),
        MUNICIPIO VARCHAR(50),
        CAPITAL INTEGER,
        CODIGO_DANE INTEGER,
        AGRUPA_EDAD_PERSONA VARCHAR(50),
        GENERO VARCHAR(50),
        ARMAS_MEDIOS VARCHAR(50),
        CANTIDAD INTEGER,
        DELITO VARCHAR(250),
        CRIMEN VARCHAR(255)
    );
    '''
    try:
        with conn.cursor() as cur:
            # Elimina la tabla si existe
            cur.execute(drop_table_query)
            # Confirma los cambios
            conn.commit()
            # Ejecuta la consulta para crear la tabla
            cur.execute(create_table_query)
            # Confirma los cambios
            conn.commit()
            print("Tabla creada exitosamente.")
    except psycopg2.Error as e:
        print("Error al crear la tabla:", e)


def load_data(conn, cur):
    try:
        # Ruta a la carpeta que contiene los archivos CSV
        folder_path = 'C:\\Users\\Milton\\Desktop\\POSGRADO\\Trabajo dirigido\\police-etl\\Silver'

        # Obtener la lista de archivos CSV en la carpeta
        csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

        # Contador total de inserciones
        total_insertions = 0

        # Iterar sobre los archivos CSV y cargar los datos en la tabla
        for csv_file in csv_files:
            # Ruta completa al archivo CSV
            file_path = os.path.join(folder_path, csv_file)

            # Nombre del archivo
            file_name_column = os.path.splitext(
                os.path.basename(file_path))[0].upper().replace("_", " ")

            orden = ['FECHA', 'DEPARTAMENTO', 'MUNICIPIO', 'CAPITAL', 'CODIGO_DANE', 'AGRUPA_EDAD_PERSONA',
                     'GENERO', 'ARMAS_MEDIOS', 'CANTIDAD', 'DELITO']

            # Cargar el archivo CSV en un DataFrame de pandas
            try:
                # Leer el archivo CSV en un DataFrame y ordenarlo según los criterios de OLPT.STAGE_DATA
                df = pd.read_csv(file_path, sep=',',
                                 encoding='UTF-8', header=0)
            except Exception as e:
                print(f"Error al leer el archivo {csv_file}: {e}")
                continue

            # Verificar si la columna 'DELITO' existe en el DataFrame
            if 'DELITO' not in df.columns:
                # Si la columna 'DELITO' no existe, agregar una columna con valores nulos
                df['DELITO'] = "NO REPORTADO"

            df = df[orden]

            # Agregar una columna con el nombre del archivo
            df['CRIMEN'] = file_name_column

            # Convertir el DataFrame a una lista de tuplas
            data = df.values.tolist()

            # Tamaño del lote para la inserción masiva
            batch_size = 10000  # Puedes ajustar este valor según las pruebas de rendimiento

            # Total de filas en el DataFrame
            total_rows = len(data)

            for i in range(0, total_rows, batch_size):
                batch_data = data[i:i+batch_size]

                # Consulta de inserción masiva
                insert_query = f"INSERT INTO {table_name} VALUES %s"

                try:
                    # Insertar los datos de forma masiva
                    execute_values(cur, insert_query, batch_data)

                    # Incrementar el contador total de inserciones
                    total_insertions += len(batch_data)

                    # Calcular el porcentaje de progreso
                    progress_percentage = (
                        (i + len(batch_data)) / total_rows) * 100

                    # Imprimir el progreso de la inserción en la misma línea
                    print(f"Archivo: {csv_file}, Progreso: {
                          progress_percentage:.2f}%", end='\r')

                except psycopg2.Error as e:
                    print(f"\nError al insertar datos del archivo {
                          csv_file} en la base de datos: {e}")
                    conn.rollback()
                    break

            cur.execute(
                "UPDATE OLTP.STAGE_DATA SET MUNICIPIO = 'CHIBOLO' WHERE MUNICIPIO = 'CHIVOLO';")

            # Confirmar los cambios después de cada archivo
            conn.commit()

            # Imprimir una nueva línea al final del archivo
            print()

        # Imprimir el total de inserciones realizadas
        print(f"Total de inserciones realizadas: {total_insertions}")

    except (psycopg2.Error, pd.errors.EmptyDataError) as e:
        print("Error al insertar datos en la base de datos:", e)


try:
    # Estableciendo la conexión
    with psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    ) as conn:
        print("Conexión exitosa")
        with conn.cursor() as cur:
            create_table(conn, table_name)
            load_data(conn, cur)

    # Cerrar la conexión
    conn.close()

except psycopg2.Error as e:
    print("Error al conectar a la base de datos:", e)
