import datetime
import random
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import create_engine

# Datos de conexión a la base de datos
db_host = "localhost"
db_name = "Pruebas"
db_user = "postgres"
db_password = "123456789"
# Puerto predeterminado de PostgreSQL
db_port = "5432"


def truncate_dimensions(conn):
    try:
        with conn.cursor() as cur:

            # Lista de tablas a truncar
            tablas = [
                "historic",
                "departments",
                "locations",
                "towns",
                "agegroups",
                "crimes",
                "article",
                "time",
                "weapons"
            ]

            # Comenzar una transacción
            conn.autocommit = False

            # Ejecutar el truncado de cada tabla con CASCADE
            for tabla in tablas:
                cur.execute(f"LOCK TABLE OLTP.{tabla} IN EXCLUSIVE MODE;")
                cur.execute(f"TRUNCATE TABLE OLTP.{tabla} CASCADE;")

            # Confirmar la transacción
            conn.commit()
            print("Truncado de tablas con éxito.")

    except psycopg2.Error as e:
        # Revertir la transacción en caso de error
        conn.rollback()
        print("Error al truncar las tablas:", e)


def create_dimension_crimes(conn):
    try:
        # Cursor para ejecutar consultas
        with conn.cursor() as cur:
            # Consulta SQL para obtener los valores únicos de la columna crimen
            query = "SELECT DISTINCT CRIMEN FROM OLTP.STAGE_DATA ORDER BY CRIMEN;"

            # Ejecutar la consulta
            cur.execute(query)

            # Iterar sobre los resultados y insertarlos en la tabla OLTP.Crimes
            for row in cur.fetchall():
                w_description = row[0]  # El valor único de crimen
                # Insertar el valor en la tabla OLTP.Crimes
                insert_query = "INSERT INTO OLTP.Crimes (W_description) VALUES (%s);"
                cur.execute(insert_query, (w_description,))

            # Confirmar los cambios
            conn.commit()
        print("Dimensión 'Crimes' creada exitosamente")
    except psycopg2.Error as e:
        print("Error al insertar datos en la base de datos:", e)


def create_dimension_weapons(conn):
    try:
        # Cursor para ejecutar consultas
        with conn.cursor() as cur:
            # Consulta SQL para obtener los valores únicos de la columna armas_medios
            query = "SELECT DISTINCT ARMAS_MEDIOS FROM OLTP.STAGE_DATA ORDER BY ARMAS_MEDIOS;"

            # Ejecutar la consulta
            cur.execute(query)

            # Iterar sobre los resultados y insertarlos en la tabla OLTP.Weapons
            for row in cur.fetchall():
                weapon_description = row[0]  # El valor único de armas_medios
                # Insertar el valor en la tabla OLTP.Weapons
                insert_query = "INSERT INTO OLTP.Weapons (W_description) VALUES (%s);"
                cur.execute(insert_query, (weapon_description,))

            # Confirmar los cambios
            conn.commit()
        print("Dimensión 'Weapons' creada exitosamente")
    except psycopg2.Error as e:
        print("Error al insertar datos en la base de datos:", e)


def create_dimension_age_groups(conn):
    try:
        # Cursor para ejecutar consultas
        with conn.cursor() as cur:
            # Consulta SQL para obtener los valores únicos de la columna agrupa_edad_persona
            query = "SELECT DISTINCT AGRUPA_EDAD_PERSONA FROM OLTP.STAGE_DATA;"

            # Ejecutar la consulta
            cur.execute(query)

            # Iterar sobre los resultados y insertarlos en la tabla OLTP.AgeGroups
            for row in cur.fetchall():
                # El valor único de agrupa_edad_persona
                age_group_description = row[0]
                # Insertar el valor en la tabla OLTP.AgeGroups
                insert_query = "INSERT INTO OLTP.AgeGroups (AG_description) VALUES (%s);"
                cur.execute(insert_query, (age_group_description,))

            # Confirmar los cambios
            conn.commit()
        print("Dimensión 'AgeGroups' creada exitosamente")
    except psycopg2.Error as e:
        print("Error al insertar datos en la base de datos:", e)


def create_dimension_departments(conn):
    try:
        # Cursor para ejecutar consultas
        with conn.cursor() as cur:
            # Consulta SQL para obtener los valores únicos de la columna departamento
            query = "SELECT DISTINCT UPPER(DEPARTAMENTO) AS DEPARTAMENTO FROM OLTP.STAGE_DATA ORDER BY DEPARTAMENTO;"

            # Ejecutar la consulta
            cur.execute(query)

            # Iterar sobre los resultados y insertarlos en la tabla OLTP.Departments
            for row in cur.fetchall():
                department_name = row[0]  # El valor único de departamento
                # Insertar el valor en la tabla OLTP.Departments
                insert_query = "INSERT INTO OLTP.Departments (Department_name) VALUES (%s);"
                cur.execute(insert_query, (department_name,))

            # Confirmar los cambios
            conn.commit()
        print("Dimensión 'Departments' creada exitosamente")
    except psycopg2.Error as e:
        print("Error al insertar datos en la base de datos:", e)


def create_dimension_towns(conn):
    try:
        # Cursor para ejecutar consultas
        with conn.cursor() as cur:
            # Consulta SQL para obtener los valores únicos de la columna municipio
            query = "SELECT DISTINCT UPPER(MUNICIPIO) AS MUNICIPIO FROM OLTP.STAGE_DATA ORDER BY MUNICIPIO;"

            # Ejecutar la consulta
            cur.execute(query)

            # Iterar sobre los resultados e insertarlos en la tabla OLTP.Towns
            for row in cur.fetchall():
                town_name = row[0]  # El valor único de municipio
                # Insertar el valor en la tabla OLTP.Towns
                insert_query = "INSERT INTO OLTP.Towns (Town_name) VALUES (%s);"
                cur.execute(insert_query, (town_name,))

            # Confirmar los cambios
            conn.commit()
        print("Dimensión 'Towns' creada exitosamente")
    except psycopg2.Error as e:
        print("Error al insertar datos en la base de datos:", e)


def create_dimension_articles(conn):
    try:
        # Cursor para ejecutar consultas
        with conn.cursor() as cur:
            # Consulta SQL para obtener los valores únicos de la columna DELITO
            query = "SELECT DISTINCT DELITO FROM OLTP.STAGE_DATA WHERE DELITO IS NOT NULL;"

            # Ejecutar la consulta
            cur.execute(query)

            # Iterar sobre los resultados e insertarlos en la tabla OLTP.ARTICLE
            for row in cur.fetchall():
                article_name = row[0]  # El valor único de DELITO
                # Insertar el valor en la tabla OLTP.ARTICLE
                insert_query = "INSERT INTO OLTP.ARTICLE (ARTICLE_NAME) VALUES (%s);"
                cur.execute(insert_query, (article_name,))

            # Confirmar los cambios
            conn.commit()
        print("Dimensión 'Article' creada exitosamente")
    except psycopg2.Error as e:
        print("Error al insertar datos en la base de datos:", e)


def generar_datos_fecha(fecha):
    num_anio = fecha.year
    num_mes = fecha.month
    num_dia = fecha.day
    num_semana_mes = (fecha.day - 1) // 7 + 1

    if num_mes <= 6:
        str_semestre = "Primero"
    else:
        str_semestre = "Segundo"

    str_mes = fecha.strftime("%B")
    num_periodo = (num_mes - 1) // 3 + 1

    return (
        fecha,
        num_anio,
        str_semestre,
        num_periodo,
        str_mes,
        num_mes,
        num_dia,
        num_semana_mes
    )


def insertar_datos_masivos(cursor, datos):
    query = """
    INSERT INTO OLTP.TIME (
        DT_FECHA, NUM_ANIO, STR_SEMESTRE, NUM_PERIODO, STR_MES, NUM_MES, NUM_DIA, NUM_SEMANA_MES
    ) VALUES %s
    """
    execute_values(cursor, query, datos)


def create_dimension_time(conn):
    try:
        # Cursor para ejecutar consultas
        with conn.cursor() as cur:
            fecha_inicio = datetime.datetime(2013, 1, 1)
            fecha_fin = datetime.datetime(2023, 12, 31)
            delta = fecha_fin - fecha_inicio
            batch_size = 1000  # Tamaño del lote para la inserción masiva
            datos = []

            for i in range(delta.days + 1):
                fecha_actual = fecha_inicio + datetime.timedelta(days=i)
                datos.append(generar_datos_fecha(fecha_actual))

                if len(datos) >= batch_size:
                    insertar_datos_masivos(cur, datos)
                    datos = []  # Limpiar la lista después de la inserción masiva

            # Insertar cualquier dato restante
            if datos:
                insertar_datos_masivos(cur, datos)

            # Confirmar los cambios
            conn.commit()
        print("Dimensión 'Time' creada exitosamente")
    except psycopg2.Error as e:
        print("Error al insertar datos en la base de datos:", e)


def create_dimension_locations(conn):
    try:
        # Cursor para ejecutar consultas
        with conn.cursor() as cur:
            # Consulta SQL para obtener los valores únicos necesarios
            query = """SELECT DISTINCT CODIGO_DANE, UPPER(DEPARTAMENTO) AS DEPARTAMENTO, UPPER(MUNICIPIO) AS MUNICIPIO 
                        FROM OLTP.STAGE_DATA ORDER BY CODIGO_DANE, DEPARTAMENTO, MUNICIPIO;"""
            # Ejecutar la consulta
            cur.execute(query)

            # Iterar sobre los resultados
            for row in cur.fetchall():
                dane_id = row[0]

                department_name = row[1]
                town_name = row[2]

                # Obtener DEPARTMENT_ID de la tabla DEPARTMENTS
                cur.execute(
                    "SELECT DEPARTMENT_ID FROM OLTP.DEPARTMENTS WHERE DEPARTMENT_NAME = %s;", (department_name,))
                department_id = cur.fetchone()[0]

                # Obtener TOWN_ID de la tabla TOWNS
                cur.execute(
                    "SELECT TOWN_ID FROM OLTP.TOWNS WHERE TOWN_NAME = %s;", (town_name,))
                town_id = cur.fetchone()[0]

                # Insertar el valor en la tabla OLTP.LOCATIONS
                insert_query = "INSERT INTO OLTP.LOCATIONS (DANE_ID, DEPARTMENT_ID, TOWN_ID) VALUES (%s, %s, %s);"
                cur.execute(insert_query, (dane_id, department_id, town_id))

            # Confirmar los cambios
            conn.commit()
        print("Dimensión 'LOCATIONS' creada exitosamente")
    except psycopg2.Error as e:
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
        truncate_dimensions(conn)
        create_dimension_crimes(conn)
        create_dimension_weapons(conn)
        create_dimension_age_groups(conn)
        create_dimension_departments(conn)
        create_dimension_towns(conn)
        create_dimension_articles(conn)
        create_dimension_time(conn)
        create_dimension_locations(conn)
        # create_fact_table(conn)
except psycopg2.Error as e:
    print("Error al conectar a la base de datos:", e)
