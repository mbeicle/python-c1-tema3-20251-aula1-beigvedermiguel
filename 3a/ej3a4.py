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
DB_NAME = "biblioteca"
MONGODB_PORT = 0
MONGODB_HOST = ""
MONGODB_USERNAME = ""
MONGODB_PASSWORD = ""

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
    pass

def crear_colecciones(db: pymongo.database.Database) -> None:
    """
    Crea las colecciones necesarias para la biblioteca.
    En MongoDB, no es necesario definir el esquema de antemano,
    pero podemos crear índices para optimizar el rendimiento.
    """
    # Debes crear colecciones para 'autores' y 'libros'
    # 1. Crear colección de autores con índice por nombre
    # 2. Crear colección de libros con índices
    pass

def insertar_autores(db: pymongo.database.Database, autores: List[Tuple[str]]) -> List[str]:
    """
    Inserta varios autores en la colección 'autores'
    """
    # Debes realizar los siguientes pasos:
    # 1. Convertir las tuplas a documentos
    # 2. Insertar los documentos
    # 3. Devolver los IDs como strings
    pass

def insertar_libros(db: pymongo.database.Database, libros: List[Tuple[str, int, str]]) -> List[str]:
    """
    Inserta varios libros en la colección 'libros'
    """
    # Debes realizar los siguientes pasos:
    # 1. Convertir las tuplas a documentos
    # 2. Insertar los documentos
    # 3. Devolver los IDs como strings
    pass

def consultar_libros(db: pymongo.database.Database) -> None:
    """
    Consulta todos los libros y muestra título, año y nombre del autor
    """
    # Debes realizar los siguientes pasos:
    # 1. Realizar una agregación para unir libros con autores
    # 2. Mostrar los resultados
    pass

def buscar_libros_por_autor(db: pymongo.database.Database, nombre_autor: str) -> List[Tuple[str, int]]:
    """
    Busca libros por el nombre del autor
    """
    # Debes realizar los siguientes pasos:
    # 1. Primero encontrar el autor y buscar todos los libros del autor
    # 2. Convertir a lista de tuplas (titulo, anio)
    pass

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
    # 2. Realizar la actualización
    pass

def eliminar_libro(
        db: pymongo.database.Database,
        id_libro: str
) -> bool:
    """
    Elimina un libro por su ID
    """
    # Debes eliminar el libro con el ID proporcionado
    pass

def ejemplo_transaccion(db: pymongo.database.Database) -> bool:
    """
    Demuestra el uso de operaciones agrupadas
    """
    # Debes realizar los siguientes pasos:
    # 1. Insertar un nuevo autor
    # 2. Insertar dos libros del autor
    # Intentar limpiar los datos en caso de error
    pass


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

        # TODO: Implementar el código para probar las funciones

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Cerrar la conexión a MongoDB
        if db:
            print("\nConexión a MongoDB cerrada.")

        # Detener el proceso de MongoDB si lo iniciamos nosotros
        if mongodb_proceso:
            print("Deteniendo MongoDB...")
            detener_mongodb_docker()

            print("MongoDB detenido correctamente.")
