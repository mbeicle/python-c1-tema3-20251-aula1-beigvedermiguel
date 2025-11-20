"""
Enunciado:
En este ejercicio aprenderás a utilizar MongoDB con Python para trabajar
con bases de datos NoSQL. MongoDB es una base de datos orientada a documentos que
almacena datos en formato similar a JSON (BSON).

Tareas:
1. Conectar a una base de datos MongoDB
2. Crear colecciones (equivalentes a tablas en SQL)
3. Insertar, actualizar, consultar y eliminar documentos
4. Manejar transacciones y errores

Este ejercicio se enfoca en las operaciones básicas de MongoDB desde Python utilizando PyMongo.
"""

import subprocess
import time
import os
import sys
from typing import List, Tuple, Optional

import pymongo
from bson.objectid import ObjectId

# Configuración de MongoDB (la debes obtener de "docker-compose.yml"):
DB_NAME = 'biblioteca'
MONGODB_PORT = 27017
MONGODB_HOST = 'localhost'
MONGODB_USERNAME = 'testuser'
MONGODB_PASSWORD = 'testpass'

def verificar_docker_instalado() -> bool:
    """
    Verifica si Docker está instalado en el sistema y el usuario tiene permisos
    """
    try:
        # Verificar si docker está instalado
        result = subprocess.run(["docker", "--version"],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               text=True)
        if result.returncode != 0:
            return False

        # Verificar si docker-compose está instalado
        result = subprocess.run(["docker", "compose", "version"],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               text=True)
        if result.returncode != 0:
            return False

        # Verificar permisos de Docker
        result = subprocess.run(["docker", "ps"],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False

def iniciar_mongodb_docker() -> bool:
    """
    Inicia MongoDB usando Docker Compose
    """
    try:
        # Obtener la ruta al directorio actual donde está el docker-compose.yml
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # Detener cualquier contenedor previo
        subprocess.run(
            ["docker", "compose", "down"],
            cwd=current_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )

        # Iniciar MongoDB con docker-compose
        result = subprocess.run(
            ["docker", "compose", "up", "-d"],
            cwd=current_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        if result.returncode != 0:
            print(f"Error al iniciar MongoDB: {result.stderr}")
            return False

        # Dar tiempo para que MongoDB se inicie completamente
        time.sleep(5)
        return True

    except subprocess.CalledProcessError as e:
        print(f"Error al ejecutar Docker Compose: {e}")
        return False
    except Exception as e:
        print(f"Error inesperado: {e}")
        return False

def detener_mongodb_docker() -> None:
    """
    Detiene el contenedor de MongoDB
    """
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        subprocess.run(
            ["docker", "compose", "down"],
            cwd=current_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
    except Exception as e:
        print(f"Error al detener MongoDB: {e}")

def crear_conexion() -> pymongo.database.Database:
    """
    Crea y devuelve una conexión a la base de datos MongoDB
    """
    # Debes conectarte a la base de datos MongoDB usando PyMongo
    CONNECTION_STRING = f'mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}/'
    client = pymongo.MongoClient(CONNECTION_STRING)
    # Creamos la base de datos
    db = client[DB_NAME]
    return db

def crear_colecciones(db: pymongo.database.Database) -> None:
    """
    Crea las colecciones necesarias para la biblioteca.
    En MongoDB, no es necesario definir el esquema de antemano,
    pero podemos crear índices para optimizar el rendimiento.
    """
    # Debes crear colecciones para 'autores' y 'libros'
    # 1. Crear colección de autores con índice por nombre
    collection_autores = db["autores"]
    # Creamos un índice ascendente en el campo 'nombre', el '1' significa orden ascendente
    collection_autores.create_index([('nombre', 1)])
    # 2. Crear colección de libros con índices
    collection_libros = db["libros"]
    # Creamos un índice ascendente en el campo 'nombre' - El '1' indica orden ascendente
    collection_libros.create_index([('titulo', 1)])
    collection_libros.create_index([('anio', 1)])
    collection_libros.create_index([('autor_id', 1)])

def insertar_autores(db: pymongo.database.Database, autores: List[Tuple[str]]) -> List[str]:
    """
    Inserta varios autores en la colección 'autores'
    """
    # Debes realizar los siguientes pasos:
    dict_autores = []
    # 1. Convertir las tuplas a documentos
    for autor in autores:
        dict_autor = {'nombre':autor[0]}
        dict_autores.append(dict_autor)
    # 2. Insertar los documentos
    resultado = db.autores.insert_many(dict_autores)
    # 3. Devolver los IDs como strings
    id = [str(id) for id in resultado.inserted_ids]
    return id

def insertar_libros(db: pymongo.database.Database, libros: List[Tuple[str, int, str]]) -> List[str]:
    """
    Inserta varios libros en la colección 'libros'
    """
    # Debes realizar los siguientes pasos:
    # 1. Convertir las tuplas a documentos
    dict_libros = [{'titulo':libro[0],
                    'anio':libro[1],
                    'autor_id': ObjectId(libro[2]) if isinstance(libro[2], str) else libro[2]
                }
                for libro in libros
                ]
    # 2. Insertar los documentos
    resultado = db.libros.insert_many(dict_libros)
    # 3. Devolver los IDs como strings
    id = [str(id) for id in resultado.inserted_ids]
    return id

def consultar_libros(db: pymongo.database.Database) -> None:
    """
    Consulta todos los libros y muestra título, año y nombre del autor
    """
    # Debes realizar los siguientes pasos:
    # 1. Realizar una agregación para unir libros con autores
    # pipeline de agregación que procesa documentos de la colección 'libros'
    # y de la colección autores.
    # Busca documentos en la colección autores donde el campo '_id' coincida con 
    # el valor del campo 'autor_id' en los documentos de la colección libros.
    # Los documentos coincidentes de autores se añaden a los documentos de
    # libros en un nuevo campo de tipo array llamado autor.
    pipeline = [
        {
        '$lookup': {
            'from': 'autores',          # La colección a unir a libros
            'localField': 'autor_id',   # Campo de 'libros'
            'foreignField': '_id',      # Campo de 'autores'
            'as': 'autor'               # Nombre del nuevo array que contendrá los documentos unidos
            }
        },
    
    # Para cada elemento en el array 'autor', se genera un nuevo documento de
    # salida. Esto es útil para aplanar la estructura de datos y permitir
    # un fácil acceso a los campos del autor en etapas posteriores.
        {
            '$unwind': '$autor'
        },

    # Aquí se reforman los documentos de salida, seleccionando campos
    # específicos y creando uno nuevo.
    # Incluye los campos titulo y anio (el valor 1 indica que se incluyen,
    # (_id se excluye por defecto en $project a menos que se especifique lo contrario).
    # También se crea un nuevo campo llamado 'autor_nombre' que toma el valor
    # del campo 'nombre' dentro del documento del autor ($autor.nombre).
        {
            '$project': {
                'titulo': 1,
                'anio': 1,
                'autor_nombre': '$autor.nombre'
            }
        },
    # Esta etapa ordena los documentos resultantes, primero por el campo
    # 'autor_nombre' en orden ascendente (1) y, si varios documentos tienen el
    # mismo 'autor_nombre', se ordenan adicionalmente por el campo titulo
    # en orden ascendente (1).
        {
            '$sort': {'autor_nombre': 1, 'titulo': 1}
        }
    ]
    # Ejecuta el pipeline y almacena los resultados
    resultados = db.libros.aggregate(pipeline)
    # 2. Mostrar los resultados
    for libro in resultados:
        print(f'Título: {libro['titulo']} - Año: {libro['anio']} - Autor: {libro['autor_nombre']}')

def buscar_libros_por_autor(db: pymongo.database.Database, nombre_autor: str) -> List[Tuple[str, int]]:
    """
    Busca libros por el nombre del autor
    """
    # Debes realizar los siguientes pasos:
    # 1. Primero encontrar el autor y buscar todos los libros del autor
    autor = db.autores.find_one({"nombre": nombre_autor})
    if not autor:
        return []
    # pipeline de agregación que filtra los documentos de la colección 'libros'
    # que sean del autor.
    pipeline = [
        {
            '$match': {'autor_id': autor['_id']}
        },
    # Se ordenan los documentos resultantes, por el campo 'anio' en orden ascendente (1)
        {
            '$sort': {'anio': 1}
        },
    # Se reforman los documentos de salida, seleccionando campos específicos
        {
            '$project': {
                'titulo': 1,
                'anio': 1,
                '_id': 0
            }
        }
    ]
    # Se ejecuta el pipeline y se almacenan los resultados
    resultados = db.libros.aggregate(pipeline)
    # 2. Convertir a lista de tuplas (titulo, anio)
    libros_autor = [(libro['titulo'], libro['anio']) for libro in resultados]
    return libros_autor

def actualizar_libro(
        db: pymongo.database.Database,
        id_libro: str,
        nuevo_titulo: Optional[str]=None,
        nuevo_anio: Optional[int]=None
) -> bool:
    """
    Actualiza la información de un libro existente
    """
    # Debes realizar los siguientes pasos:
    # 1. Crear diccionario de actualización
    actualizacion = {}
    if nuevo_titulo is not None:
        actualizacion['titulo'] = nuevo_titulo
    if nuevo_anio is not None:
        actualizacion['anio'] = nuevo_anio
    
    if not actualizacion:
        return True
    # 2. Realizar la actualización
    # Filtramos el documento por su _id
    # El operador '$set' añade los campos si no existen
    resultado = db.libros.update_one(
        {'_id': ObjectId(id_libro)},
        {'$set': actualizacion}
    )
    # Si el atributo 'modified.count'es mayor que 0, es que se ha realizado
    # alguna operación de actualización
    return resultado.modified_count > 0

def eliminar_libro(
        db: pymongo.database.Database,
        id_libro: str
) -> bool:
    """
    Elimina un libro por su ID
    """
    # Debes eliminar el libro con el ID proporcionado
    resultado = db.libros.delete_one({'_id': ObjectId(id_libro)})
    # Muestra cuántos documentos se eliminaron
    print(f'\nDocumentos eliminados: {resultado.deleted_count}')
    return resultado.deleted_count > 0

def ejemplo_transaccion(db: pymongo.database.Database) -> bool:
    """
    Demuestra el uso de operaciones agrupadas
    """
    # Debes realizar los siguientes pasos:
    # 1. Insertar un nuevo autor
    try:
        autor = {"nombre": "J.R.R. Tolkien"}
        # Insertamos el documento
        resultado = db.autores.insert_one(autor)
        autor_id = resultado.inserted_id
    # 2. Insertar dos libros del autor
        libros = [{
                "titulo": "El hobbit",
                "anio": 1937,
                "autor_id": autor_id
                },
                {
                "titulo": "El Señor de los Anillos",
                "anio": 1954,
                "autor_id": autor_id
                }
                ]
        db.libros.insert_many(libros)
        return True
    # Intentar limpiar los datos en caso de error
    except Exception as e:
        print(f"Error en la transacción: {e}")
        # Intentar limpiar los datos en caso de error
        try:
            if 'autor_id' in locals():
                db.autores.delete_one({"_id": autor_id})
                db.libros.delete_many({"autor_id": autor_id})
        except:
            pass
        return False


if __name__ == "__main__":
    mongodb_proceso = None
    db = None

    try:
        # Verificar si Docker está instalado
        if not verificar_docker_instalado():
            print("Error: Docker no está instalado o no está disponible en el PATH.")
            print("Por favor, instala Docker y asegúrate de que esté en tu PATH.")
            sys.exit(1)

        # Iniciar MongoDB usando Docker
        print("Iniciando MongoDB con Docker...")
        if not iniciar_mongodb_docker():
            print("No se pudo iniciar MongoDB. Asegúrate de tener los permisos necesarios.")
            sys.exit(1)

        print("MongoDB iniciado correctamente.")

        # Crear una conexión
        print("Conectando a MongoDB...")
        db = crear_conexion()
        print("Conexión establecida correctamente.")

        # Crear colecciones
        crear_colecciones(db)
        print("Colecciones creadas correctamente.")

        # Insertar autores en la colección 'autores'
        autores = [
                  ("Gabriel García Márquez",),
                  ("Isabel Allende",),
                  ("Jorge Luis Borges",)
                ]

        autores_id = insertar_autores(db, autores)
        print(f'Autores con id: {autores_id} insertados correctamente.')

        # Insertar libros en la colección 'libros'
        # Datos de libros
        libros = [
                ("Cien años de soledad", 1967, autores_id[0]),
                ("El amor en los tiempos del cólera", 1985, autores_id[0]),
                ("La casa de los espíritus", 1982, autores_id[1]),
                ("Paula", 1994, autores_id[1]),
                ("Ficciones", 1944, autores_id[2]),
                ("El Aleph", 1949, autores_id[2])
            ]
        libros_id = insertar_libros(db, libros)
        print(f'Libros {libros_id} insertados correctamente.')

        # Consultar libros de la colección 'libros' muestra título, año y nombre del autor
        print('\nLibros incluidos en la colección: ')
        consultar_libros(db)

        # Buscar libros por el nombre del autor
        libros_autor = buscar_libros_por_autor(db, 'Gabriel García Márquez')
        print('\nLibros de Gabriel García Márquez: ')
        for libro in libros_autor:
            print(f"- {libro[0]}, {libro[1]}")

        # Primero obtenemos el ID del primer libro
        primer_libro = db.libros.find_one({"titulo": "Cien años de soledad"})
        if primer_libro is not None:
            libro_id = str(primer_libro["_id"])
        # Llamamos a la función 'actualizar_libro'
        result = actualizar_libro(db, libro_id, nuevo_titulo='Titulo actualizado', nuevo_anio=2025)
        if result:
            print('\nLibro actualizado correctamente:')
            # Verificar que el libro se actualizó correctamente
            libro_actualizado = db.libros.find_one({"_id": ObjectId(libro_id)})
            print(f'Titulo: {libro_actualizado['titulo']}, Año: {libro_actualizado['anio']}')

        # Ahora obtenemos el ID del último libro
        ultimo_libro = db.libros.find_one({"titulo": "El Aleph"})
        if ultimo_libro is not None:
            libro_id = str(ultimo_libro["_id"])
        # Llamamos a la función 'eliminar_libro'
        result = eliminar_libro(db, libro_id)
        if result:
            print('\nLibros que restan en la base de datos:')
            # Verificar los libros que quedan
            consultar_libros(db)

        # Operaciones agrupadas
        result = ejemplo_transaccion(db)
        if result:
            print('Operaciones realizadas correctamente')
            print('\nLibros en la base de datos:')
            # Mostrar los libros después de la modificación
            consultar_libros(db)
        
        # TODO: Implementar el código para probar las funciones

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Cerrar la conexión a MongoDB
        if db == None:
            print("\nConexión a MongoDB cerrada.")

        # Detener el proceso de MongoDB si lo iniciamos nosotros
        if mongodb_proceso:
            print("Deteniendo MongoDB...")
            detener_mongodb_docker()

            print("MongoDB detenido correctamente.")
