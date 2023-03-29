#Generando un millon de alumnos e insertandolos en mongodb usando hilos
import random
from faker import Faker
import pymongo
import json
import threading

faker = Faker()

# Definir los primeros 6 dígitos y la cantidad de números a generar
primeros_6_digitos = "192501"
cantidad_numeros = 1000

# Generar los números de control aleatorios
numero_control = []
for i in range(cantidad_numeros):
    ultimos_4_digitos = str(i+1).zfill(4) # Convertir el número de iteración en un string de 4 dígitos rellenado con ceros a la izquierda
    numero_control.append(f"{primeros_6_digitos}{ultimos_4_digitos}")
   # print(numero_control)

# Generar mil nombres aleatorios
nombre =[]
for i in range(1000):
    nombre.append(faker.name())
  #  print(nombre)

# Generar mil direcciones aleatorias
direcciones = []
for i in range(1000):
    direccion = {
        "calle": faker.street_name(),
        "num_exterior": faker.building_number(),
        "num_interior": faker.random_int(min=1, max=50),
        "colonia": faker.street_suffix(),
        # Agregar otros campos aleatorios según sea necesario
    }
    direcciones.append(direccion)

#for direccion in direcciones:
#    print(direccion)

#Generar CP
codigos_postales = []
for i in range(1000):
    codigo_postal = f"810{faker.random_int(min=0, max=99):02d}"
    codigos_postales.append(codigo_postal)

# Imprimir los códigos postales generados
#for codigo_postal in codigos_postales:
#    print(codigo_postal)

# Generar mil registros aleatorios con el tipo de sexo
generos = []
for i in range(1000):
    genero = faker.random_element(elements=('H', 'M', 'No binario'))
    generos.append(genero)

# Imprimir los géneros generados
#for genero in generos:
#    print(genero)

# Generar mil números de teléfono 687
numeros_telefono = []
for i in range(1000):
    numero_telefono = f"687{faker.random_int(min=1000000, max=9999999):07d}"
    numeros_telefono.append(numero_telefono)

# Imprimir los números de teléfono generados
#for numero_telefono in numeros_telefono:
#    print(numero_telefono)

#Numero de seguro social
seguro = []
while len(seguro) < 1000:
    numeros = [random.randint(0, 9) for _ in range(10)]
    numero_verificador = random.randint(0, 9)
    registro = "".join(map(str, numeros)) + "-" + str(numero_verificador)
    seguro.append(registro)

# Imprimir los registros generados
#for registro in registros:
#   print(seguro)

#Tipos de sangre
registros = ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-']
tiposSangre = []

for i in range(1000):
    tipo_sangre = random.choice(registros)
    tiposSangre.append(tipo_sangre)

#print(tiposSangre)

#Carreras
registros = ['Sistemas Computacionales', 'Innovacion Agricola', 'Industrial', 'Gestion Empresarial', 'Industrias Alimentarias', 'Mecanica']
carreras = []

for i in range(1000):
    carrera = random.choice(registros)
    carreras.append(carrera)
#print(carreras)

#Grupos
registrosGrupo = ['101', '201', '301', '401', '501', '601', '701', '801', '102', '202', '302', '402', '502', '602', '702', '802']
grupos = []
for i in range(1000):
    grupo = random.choice(registrosGrupo)
    grupos.append(grupo)
#print(grupos)

materias = ['Programacion web', 'Administracion de redes', 'Desarrollo de aplicaciones', 'Ciencia de datos', 'Taller de investigacion', 'Big Data', 'IoT']
estados = ['Activo', 'Inactivo']
materiasResult = []

for i in range(1000):
    result = {}
    for materia in materias:
        result[materia] = random.choice(estados)
    materiasResult.append(result)

#print(materiasResult)

#Actividades deportivas
actividades_deportivas = ['futbol', 'baloncesto', 'voleibol', 'natacion', 'tenis', 'beisbol', 'atletismo', 'boxeo', 'gimnasia']
actividadesResult = []

for i in range(1000):
    actividad1 = random.choice(actividades_deportivas)
    actividad2 = random.choice(actividades_deportivas)
    while actividad1 == actividad2:
        actividad2 = random.choice(actividades_deportivas)
    registro = actividad1 + ' y ' + actividad2
    actividadesResult.append(registro)

#print(actividadesResult)

#Turno
turnos = ['Matutino', 'Vespertino']
turnosResult = []
for i in range(1000):
    turno = random.choice(turnos)
    turnosResult.append(turno)
#print(turnosResult)

data = {
    "numeroControl": "",
    "nombre": "",
    "edad" :0,
    "domicilio": "",
    "cp":"",
    "sexo":"",
    "telefono": "",
    "seguro":"",
    "tipoSangre":"",
    "carrera":"",
    "grupo":"",
    "materias":"",
    "actividadesDeportivas":"",
    "turno":""
}

datos_json = []
for i in range(1000001):
    obj = data.copy()
    obj["numeroControl"] = random.choice(numero_control)
    obj["nombre"] = random.choice(nombre)
    obj["edad"] = random.randint(18, 60)
    obj["domicilio"] = random.choice(direcciones)
    obj["cp"] = random.choice(codigos_postales)
    obj["sexo"] = random.choice(generos)
    obj["telefono"] = random.choice(numeros_telefono)
    obj["seguro"] = random.choice(seguro)
    obj["tipoSangre"] = random.choice(tiposSangre)
    obj["carrera"] = random.choice(carreras)
    obj["grupo"] = random.choice(grupos)
    obj["materias"] = random.choice(materiasResult)
    obj["actividadesDeportivas"] =  random.choice(actividadesResult)
    obj["turno"] = random.choice(turnosResult)
    datos_json.append(obj)
    print(i)


# Función para insertar datos en MongoDB
def insert_data(collection, data):
    for json_doc in datos_json:
        json_string = json.dumps(json_doc)
        collection.insert_one(json.loads(json_string))

# Conectamos con la base de datos de MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["Alumnos"]
collection = db["Registros"]


# Dividimos los datos en N fragmentos
n_threads = 10
chunk_size = len(datos_json) // n_threads
chunks = [datos_json[i:i+chunk_size] for i in range(0, len(datos_json), chunk_size)]

# Creamos los hilos y los ejecutamos
threads = []
for chunk in chunks:
    t = threading.Thread(target=insert_data, args=(collection, chunk))
    threads.append(t)
    t.start()

# Esperamos a que todos los hilos terminen
for t in threads:
    t.join()