from pymongo import MongoClient
import json
import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser
from flask import Flask, jsonify

app = Flask(__name__)

# ---Parte 1: Instalación y conexión de MongoDB con Python ---
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

# Consultar todos los documentos de la colección
documentos = coleccion.find()

# Imprimir los documentos encontrados
for documento in documentos:
   print(documento)

# ---Parte 4: Análisis de Datos en MongoDB ---
# Investigar herramientas de autogeneración de datos: https://www.mockaroo.com/

# Insertar JSON
with open('mascotas.json', "r", encoding="utf-8") as file:
    data=json.load(file)
    coleccion.insert_many(data)
print("json importado correctamente")

#Perros Cachorros menores a 5 años:

perrosCachorros = coleccion.find({"animal":"perro", "edad": { "$lt" : 5 }})

df=pd.DataFrame(perrosCachorros)
df.to_csv("perros_cachorros_exportados.csv", index=False)

#Hamsters Enanos Rusos Hiperactivos:

hamsterHiperactivo = coleccion.find({"nivel_actividad":"alta","animal":"hamster","raza":"enano ruso"})

df=pd.DataFrame(hamsterHiperactivo)
df.to_csv("hamsterHiperactivos_exportados.csv", index=False)

# Convertir si hay fechas en formato string dentro del dataset al formato correcto para analisis
#Formato fecha: dd/mm/aa TDA: datetime
doc_con_str = coleccion.find_one({"ultima_vacunacion": {"$type": "string"}})
if doc_con_str:
    for doc in coleccion.find({"ultima_vacunacion": {"$type": "string"}}):
        try:
            fecha = parser.parse(doc["ultima_vacunacion"])
            coleccion.update_one({"_id": doc["_id"]}, {"$set": {"ultima_vacunacion": fecha}})
        except Exception as e:
            print(f"Error al convertir documento {doc['_id']}: {e}")

#Mascotas que no se vacunan hace un año

fecha_limite = datetime.today() - timedelta(days=365)

vacunadoMayorUnAño = coleccion.find({"ultima_vacunacion": {"$lt": fecha_limite}})

df=pd.DataFrame(vacunadoMayorUnAño)
df.to_csv("vacunadoMayorUnAño_exportados.csv", index=False)

#Peces de Gran Tamaño de agua dulce que viven en acuarios Medianos

pecesGrandesDulces = coleccion.find({"tipo_agua" : {"$not" : {"$eq" : "salada"}}, "tamano" : "grande", "acuario_litros": {"$gt" : 20, "$lt" : 80}})

df=pd.DataFrame(pecesGrandesDulces)
df.to_csv("pecesGrandesDulces_exportados.csv", index=False)

#Gatos gordos menores a 6 años o mayores a 15, que miden menos de 20 centimetros

gatosGordos = coleccion.find({"animal" : "gato", "$or" : [{"edad": {"$lt" : 6 }}, {"edad" : {"$gt" : 15 }}], "peso" : 6, "altura" : {"$lt" : 20} })
df = pd.DataFrame(gatosGordos)
df.to_csv("gatosGordos_exportados.csv", index=False)

#Perros atendidos por la Dra Ing Roxana Martinez o por Lewis Hamilton
Vet_Profe_Hamilton = coleccion.find({"animal" : "perro", "veterinario": {"$in": ["Dra Ing Roxana Martinez", "Dr Lewis Hamilton"]}})

df=pd.DataFrame(Vet_Profe_Hamilton)
df.to_csv("Vet_Profe_Hamilton_exportados.csv", index=False)

print("Datos exportados correctamente" "\n")

#PowerBI

@app.route('/Mascotas', methods=['GET'])

def datos_Mascotas():
    Mascotas = list(coleccion.find({}, {"_id": 0})) 
    # Excluye el _id para evitar problemas en JSON
    return jsonify(Mascotas)

if __name__ == '__main__':
    app.run(debug=True)
