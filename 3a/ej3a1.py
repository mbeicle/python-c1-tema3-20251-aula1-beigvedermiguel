"""
Enunciado:
En este ejercicio aprenderás a utilizar la biblioteca sqlite3 de Python para trabajar
con bases de datos SQLite. SQLite es una base de datos liviana que no requiere un servidor
y almacena la base de datos completa en un solo archivo.

Tareas:
1. Conectar a una base de datos SQLite
2. Crear tablas usando SQL
3. Insertar, actualizar, consultar y eliminar datos
4. Manejar transacciones y errores

Este ejercicio se enfoca en las operaciones básicas de SQL desde Python sin utilizar un ORM.
"""

import sqlite3
import os

# Ruta de la base de datos (en memoria para este ejemplo)
# Para una base de datos en archivo, usar: 'biblioteca.db'
DB_PATH = ':memory:'

def crear_conexion():
    """
    Crea y devuelve una conexión a la base de datos SQLite
    """
    # Implementa la creación de la conexión y retorna el objeto conexión

    # Conectamos a la base de datos en memoria
    conexion = sqlite3.connect(DB_PATH)
    return conexion

def crear_tablas(conexion):
    """
    Crea las tablas necesarias para la biblioteca:
    - autores: id (entero, clave primaria), nombre (texto, no nulo)
    - libros: id (entero, clave primaria), titulo (texto, no nulo),
              anio (entero), autor_id (entero, clave foránea a autores.id)
    """
    # Implementa la creación de tablas usando SQL
    # Usa conexion.cursor() para crear un cursor y ejecutar comandos SQL
    
    # Creamos un objeto cursor
    cursor = conexion.cursor()
    # Sentencia SQL para crear la tabla 'autores'
    # Se usa IF NOT EXISTS para evitar un error si la tabla ya existe
    crear_tabla_sql = """
    CREATE TABLE IF NOT EXISTS autores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL
    )
    """
    # Ejecuta la sentencia SQL
    cursor.execute(crear_tabla_sql)
    
    # Creamos la tabla 'libros'
    crear_tabla_sql = """
    CREATE TABLE IF NOT EXISTS libros (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    titulo TEXT NOT NULL,
    anio INTEGER,
    autor_id INTEGER,
    FOREIGN KEY (autor_id) REFERENCES autores(id) 
    )
    """
    # Ejecutamos la sentencia SQL
    cursor.execute(crear_tabla_sql)
    # Confirmamos y guardamos los cambios
    conexion.commit()

def insertar_autores(conexion, autores):
    """
    Inserta varios autores en la tabla 'autores'
    Parámetro autores: Lista de tuplas (nombre,)
    """
    # Implementa la inserción de autores usando SQL INSERT
    # Usa consultas parametrizadas para mayor seguridad
    # Creamos un objeto cursor
    cursor = conexion.cursor()   
    cursor.executemany('INSERT INTO autores (nombre) VALUES (?)', autores)
    # Confirmamos y guardamos los cambios
    conexion.commit()

def insertar_libros(conexion, libros):
    """
    Inserta varios libros en la tabla 'libros'
    Parámetro libros: Lista de tuplas (titulo, anio, autor_id)
    """
    # Implementa la inserción de libros usando SQL INSERT
    # Usa consultas parametrizadas para mayor seguridad
    # Creamos un objeto cursor
    cursor = conexion.cursor()    
    cursor.executemany('INSERT INTO libros (titulo, anio, autor_id) VALUES (?, ?, ?)', libros)
    # Confirmamos y guardamos los cambios
    conexion.commit()

def consultar_libros(conexion):
    """
    Consulta todos los libros y muestra título, año y nombre del autor
    """
    # Implementa una consulta SQL JOIN para obtener libros con sus autores
    # Imprime los resultados formateados
    # Creamos un objeto cursor
    cursor = conexion.cursor()
    # Creamos la consulta JOIN
    consultar_libros = ( """
    SELECT  libros.titulo, libros.anio, autores.nombre
    FROM autores
    JOIN libros ON autores.id = libros.autor_id
    """
    )
    # Ejecutamos la sentencia SQL
    cursor.execute(consultar_libros)
    # Obtenemos todos los resultados
    resultados = cursor.fetchall()
    # Imprimimos los resultados formateados 
    if resultados:
        for fila in resultados:
            titulo, anio, autor = fila
            print(f' Titulo: {titulo} | Año: {anio} | Autor: {autor}')
    
def buscar_libros_por_autor(conexion, nombre_autor):
    """
    Busca libros por el nombre del autor
    """
    # Implementa una consulta SQL con WHERE para filtrar por autor
    # Retorna una lista de tuplas (titulo, anio)
    # Creamos un objeto cursor
    cursor = conexion.cursor()
    # Construimos la consulta INNER JOIN - WHERE
    consulta = ( """
    SELECT libros.titulo, libros.anio
    FROM autores
    INNER JOIN libros ON autores.id = libros.autor_id 
    WHERE autores.nombre = ?
    """
    )
    # Ejecutamos la consulta
    cursor.execute(consulta, (nombre_autor,))

    # Obtenemos todos los resultados
    resultados = cursor.fetchall()
    # Devuelve los resultados 
    if resultados:
        return resultados

def actualizar_libro(conexion, id_libro, nuevo_titulo=None, nuevo_anio=None):
    """
    Actualiza la información de un libro existente
    """
    # Implementa la actualización usando SQL UPDATE
    # Solo actualiza los campos que no son None
    # Creamos un objeto cursor
    cursor = conexion.cursor()
    # Construimos por partes la consulta UPDATE - WHERE
    consulta = ("UPDATE libros SET ")
    var_updates = []
    # Añadimos las diferentes partes de la consulta 
    if nuevo_titulo is not None:
        var_updates.append(f"titulo = '{nuevo_titulo}'")
    if nuevo_anio is not None:
        var_updates.append(f"anio = '{nuevo_anio}'")
    if var_updates:
        consulta += ", ".join(var_updates)
        consulta += f"WHERE libros.id = '{id_libro}'"
    # Ejecutamos la consulta    
    cursor.execute(consulta)
    # Confirmamos y guardamos los cambios
    conexion.commit()

def eliminar_libro(conexion, id_libro):
    """
    Elimina un libro por su ID
    """
    # Implementa la eliminación usando SQL DELETE
    # Creamos un objeto cursor
    cursor = conexion.cursor()
    # Construimos la consulta DELETE - WHERE
    consulta = ("DELETE FROM libros WHERE libros.id = ?")
    # Ejecutamos la consulta    
    cursor.execute(consulta, (id_libro,))
    # Confirmamos y guardamos los cambios
    conexion.commit()

def ejemplo_transaccion(conexion):
    """
    Demuestra el uso de transacciones para operaciones agrupadas
    """
    # Implementa una transacción que:
    # 1. Comience con conexion.execute("BEGIN TRANSACTION")
    # 2. Realice varias operaciones
    # 3. Si todo está bien, confirma con conexion.commit()
    # 4. En caso de error, revierte con conexion.rollback()
    
    # Creamos un objeto cursor
    cursor = conexion.cursor()
    
    # Iniciamos las operaciones 
    try:
        # Iniciamos una transacción para agrupar operaciones
        conexion.execute('BEGIN TRANSACTION')
        # Operaciones
        cursor.execute("INSERT INTO autores (nombre) VALUES ('J.R.R. Tolkien')")
        cursor.execute("INSERT INTO libros (titulo, anio, autor_id) VALUES ('El hobbit', 1937, 4)")
        # Confirmamos la transacción
        conexion.commit()
    except sqlite3.Error as e:
        # Revertimos las operaciones en caso de error
        conexion.rollback()
        print(f"Error en la transacción: {e}")

    # Comprobamos el resultado de la transacción
    cursor.execute("SELECT autores.nombre FROM autores ")
    # Obtenemos los resultados
    resultados = cursor.fetchall()
    # Imprimimos los resultados formateados 
    if resultados:
        for fila in resultados:
            autor = fila
            print(f'Autor: {autor}')

if __name__ == "__main__":
    try:
        # Crear una conexión
        conexion = crear_conexion()

        print("Creando tablas...")
        crear_tablas(conexion)

        # Insertar autores
        autores = [
            ("Gabriel García Márquez",),
            ("Isabel Allende",),
            ("Jorge Luis Borges",)
        ]
        insertar_autores(conexion, autores)
        print("Autores insertados correctamente")

        # Insertar libros
        libros = [
            ("Cien años de soledad", 1967, 1),
            ("El amor en los tiempos del cólera", 1985, 1),
            ("La casa de los espíritus", 1982, 2),
            ("Paula", 1994, 2),
            ("Ficciones", 1944, 3),
            ("El Aleph", 1949, 3)
        ]
        insertar_libros(conexion, libros)
        print("Libros insertados correctamente")

        print("\n--- Lista de todos los libros con sus autores ---")
        consultar_libros(conexion)

        print("\n--- Búsqueda de libros por autor ---")
        nombre_autor = "Gabriel García Márquez"
        libros_autor = buscar_libros_por_autor(conexion, nombre_autor)
        print(f"Libros de {nombre_autor}:")
        for titulo, anio in libros_autor:
            print(f"- {titulo} ({anio})")

        print("\n--- Actualización de un libro ---")
        actualizar_libro(conexion, 1, nuevo_titulo="Cien años de soledad (Edición especial)")
        print("Libro actualizado. Nueva información:")
        consultar_libros(conexion)

        print("\n--- Eliminación de un libro ---")
        eliminar_libro(conexion, 6)  # Elimina "El Aleph"
        print("Libro eliminado. Lista actualizada:")
        consultar_libros(conexion)

        print("\n--- Demostración de transacción ---")
        ejemplo_transaccion(conexion)

    except sqlite3.Error as e:
        print(f"Error de SQLite: {e}")
    finally:
        if conexion:
            conexion.close()
            print("\nConexión cerrada.")
