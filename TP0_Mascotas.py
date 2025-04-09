from pymongo import MongoClient
import json
import pandas as pd

# ---Parte 1: Instalación y conexión de MongoDB con Python
try:
   # Conectar con el servidor de MongoDB (asegúrate de que esté en ejecución)
   client = MongoClient("mongodb://localhost:27017/")


except Exception as e:
   raise Exception("ERROR: ", e)

# Listar bases de datos disponibles
databases = client.list_database_names()
print("Bases de datos disponibles: ", databases)


# Crear una base de datos llamada "TPO_Mascotas"
db = client["TPO_Mascotas"]


# Crear una colección llamada "Mascotas"
coleccion = db["Mascotas"]
print("Base de datos y colección creadas con éxito.")

# ---Parte 2: Cargar y consultar datos en MongoDB

# Insertar dataset manualmente de prueba
prueba = [{"nombre": "Ejemplo", "valor": 123, "age":2}, {"nombre": "Ejemplo", "valor": 123, "age": 10}]
coleccion.insert_many(prueba)

# Consultar todos los documentos de la colección
documentos = coleccion.find()

# Imprimir los documentos encontrados
for documento in documentos:
   print(documento)

# Realizar 7 propuestas de filtros
cachorros = coleccion.find({ "age": { "$lt" : 5 }})
df=pd.DataFrame(cachorros)
df.to_csv("cachorros_exportados.csv", index=False)
print("Datos exportados correctamente")


print("Mascotas menores a 5 años de edad: ")
for documento in cachorros:
   print(documento)


# ---Parte 4: Análisis de Datos en MongoDB
# Investigar herramientas de autogeneración de datos: https://www.mockaroo.com/

# Insertar JSON
with open('mascotas.json') as file:
    data=json.load(file)
    coleccion.insert_many(data)
print("json importado correctamente")

