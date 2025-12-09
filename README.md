# Proyecto speciesdbbuild 

El Proyecto de **speciesdbbuild** se compone de dos elementos, un conjunto de scripts que crea la base de la fuente de datos (dbbuild) y un segundo elemento que compone el API de servicios (middleware_speciesv3) que disponibiliza la información bajo un estandar definido. A continuación se define cada uno de ellos.

---

## dbbuild - Sistema de Construcción de Fuente de Datos del SNIB

Este proyecto forma parte del ecosistema **SPECIES v3.0**, cuyo objetivo es estandarizar, integrar y disponibilizar información biológica y geoespacial para análisis de nicho ecológico del **Sistema Nacional de Información sobre Biodiversidad de México (SNIB)**.  
`dbbuild` es el módulo responsable de **construir la base de datos**, cargar ocurrencias, procesar variables y crear las tablas necesarias para que el middleware pueda operar.

---

## 🧬 Objetivo del Proyecto

`dbbuild` procesa datos de biodiversidad del **SNIB** y crea una base de datos PostgreSQL/PostGIS estructurada para análisis de nicho compatibles con el estándar de [species_v3.0](https://github.com/chilam-lab/species_v3.0)

---

## 📂 Estructura del Proyecto

```
dbbuild/
│
├── build_speciesdb.py        # Script principal que ejecuta todo el pipeline
├── aux_functions.py          # Funciones auxiliares (logger, lectura SQL, etc.)
├── data/                     # Archivos CSV descargados del SNIB
│   ├── columns.txt           # Lista de columnas esperadas
│   └── *.csv                 # Archivos fuente con ocurrencias
│
├── sql/
│   ├── create_extensions.sql
│   ├── create_geoportal_table.sql
│   ├── create_snib_table.sql
│   ├── create_sp_snib_table.sql
│   └── stored_procedures/    # SP que alimentan SPECIES v3.0
│
└── stored_validation/        # Validaciones geoespaciales
```

---

## 🧱 Flujo General del Pipeline

El script `build_speciesdb.py` ejecuta:

1. **Instalación de extensiones PostGIS**  
2. **Creación de tablas base**  
   - `geoportal`
   - `snib` (ocurrencias completas)
   - `sp_snib` (catálogo único de especies)
3. **Lectura incremental de archivo(s) CSV SNIB**
4. **Conversión de tipos, normalización y casting**
5. **Inserción masiva por chunks (`psycopg2.extras.execute_values`)**
6. **Carga de stored procedures**
7. **Construcción de variables bióticas en tablas normalizadas**

---

## 🧬 Tablas Principales

### 1. `snib` — Ocurrencias completas  
Contiene *cada registro* del archivo SNIB. Múltiples filas pueden pertenecer a la misma especie.

### 2. `sp_snib` — Catálogo único  
Contiene **solo una fila por especie**, derivada de `snib`.

---

## 📌 Columnas del archivo SNIB (según `columns.txt`)

> *Se listan exactamente como vienen en el archivo adjunto, sin descripción.*

```
idejemplar
numcatalogo
numcolecta
coleccion
institucion
pais
colector
fecha
dia
mes
anio
fecha2
...
(latitud, longitud, altitud, taxonomía, datos geográficos, etc.)
```

*(Lista completa incluida según archivo real.)*

---

## 📄 Ejemplo de las primeras 5 líneas del archivo SNIB

```
"1e681ab0f796b63c5dd5f0f9856b5d9f","334712","16947","COL Herbario Nacional Colombiano","ICN-UNAL Instituto de Ciencias Naturales, Universidad Nacional de Colombia","COLOMBIA","S. P. Churchill","1990-11-28","28","11","1990","","","PreservedSpecimen","","","Raz L, Agudelo H (2021)...","CC_BY_NC_4_0","gbif","18da9e5d-8966-4893-bf70-b3911152d991",...
"8497516b86a7074edb8e234d28a46d50","19973022","","Observations Observations","iNaturalist iNaturalist","","Oliver Komar","2019-01-29","29","1","2019","","","HumanObservation","",...
"3b2fcf40b7fbdb88abc78cb3da89388a","","111"," ","Ecopetrol S.A. Ecopetrol S.A.","","","2020-09-13","13","9","2020","","","HumanObservation","",...
"75fcc56929ab0c4f794773a367939e9f","112880721","","Observations Observations","iNaturalist iNaturalist","","Ricardo J. Colón-Rivera","2022-04-22","22","4","2022","","","HumanObservation","",...
"05be8f3714137ec23dc8307e490194ed","1777","9558","ICESI Herbario","ICESI Universidad Icesi","COLOMBIA","W. G. Vargas","2002-05-01","1","5","2002","","","PreservedSpecimen","",...
```

---

## 🧠 Explicación del Script Principal (`build_speciesdb.py`)

El script ejecuta varias etapas bien definidas:

### ✔️ 1. Lectura de variables de entorno
Se cargan credenciales PostgreSQL desde `.env`.

### ✔️ 2. Instalación de extensiones
Ejecuta:

```sql
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_raster;
CREATE EXTENSION postgis_topology;
```

### ✔️ 3. Creación de la tabla `geoportal`
Almacena metadatos de resolución, grid y regiones.

### ✔️ 4. Carga incremental de CSVs
- Lee el archivo SNIB en chunks (`5000` filas).
- Convierte numéricos (`latitud`, `longitud`, etc.).
- Reemplaza `NaN` → `None`.
- Inserta usando:

```python
extras.execute_values(cursor, insert_query, data_tuples)
```

Mucho más eficiente que `executemany`.

### ✔️ 5. Inserción de stored procedures
Se cargan todos los `.sql` desde `stored_procedures/`.

### ✔️ 6. Construcción de tablas bióticas
- `snib`
- `sp_snib`

> *Advertencia:* El script actualmente crea dos veces `snib` y `sp_snib`.  
> Esto se documenta para futura corrección.

---

## 🔗 Integración con SPECIES v3.0 y Middleware

Este proyecto genera la base de datos que alimenta directamente:

- Las API de [**middleware_datasources**](https://github.com/chilam-lab/middleware_datasources) que consumen variables derivadas de SNIB + WorldClim + GBIF + Regiones

El estándar de datos está definido en:

🔗 https://github.com/chilam-lab/species_v3.0

### ¿Qué provee `dbbuild` al middleware?

- Tablas con celdas por grid  
- Variables categorizadas  
- Catálogo taxonómico único  
- Ocurrencias georreferenciadas estandarizadas  
- Stored procedures para análisis de nicho

---

## ▶️ Ejecución del Proyecto

### 1. Configurar variables de entorno

Crear archivo `.env`:

```
DBNICHENAME=speciesdb
DBNICHEHOST=localhost
DBNICHEPORT=5432
DBNICHEUSER=postgres
DBNICHEPASSWD=1234
```

### 2. Ejecutar el script principal

```
python3 build_speciesdb.py
```

---

## 📌 Requisitos

- Python 3.9+
- PostgreSQL 14+
- Extensiones:
  - postgis
  - postgis_raster
  - postgis_topology
- Librerías Python:
  - psycopg2
  - pandas
  - gdal
  - python-dotenv

---

## 📜 Licencia

Uso interno dentro del ecosistema SPECIES (CONABIO + CÓDIGO C3-UNAM).  

---

## 👨‍💻 Autoría

Pipeline desarrollado como parte del proyecto **SPECIES DB** para ingestión y estandarización de datos de biodiversidad.

---


## middleware_speciesv3 - API que entrega la información de la Fuente de Datos del SNIB

Este middleware expone la información del SNIB bajo el estándar SPECIES v3.0 siguiendo las guías del repositorio oficial https://github.com/chilam-lab/species_v3.0

## Descripción general

El propósito de este middleware es estandarizar y servir la información de biodiversidad proveniente del **Sistema Nacional de Información sobre Biodiversidad de México (SNIB)**, permitiendo su integración con otras fuentes compatibles con SPECIES v3.0 para análisis de nicho, modelado ecológico y comparaciones multifuente.

El sistema implementa:

- API REST bajo Node.js + Express  
- Controladores modulares para acceso a variables, secuencias y datos  
- Rutas organizadas por funcionalidad  
- Capa de configuración y seguridad  
- Adaptación al estándar SPECIES v3.0 para interoperabilidad total

---

## Estructura del proyecto

```
middleware_speciesv3/
├── src/
│   ├── controllers/
│   │   └── snib_controller.js      # Controlador principal de la fuente SNIB
│   ├── routes/
│   │   └── snib_router.js         # Rutas expuestas por el servicio
│   ├── Utils/
│   │   ├── redisClient.js         # Cliente Redis usado para caching
│   │   └── verb_utils.js          # Funciones auxiliares
│   └── server.js                  # Arranque del servidor
├── config.js                      # Configuración del servicio
├── package.json                   # Dependencias y scripts
└── README.md                      # Documentación principal
```

---

## Controlador principal: `snib_controller.js`

Este controlador implementa toda la lógica utilizada para consultar:

- Catálogo de variables del SNIB  
- Secuencia taxonómica  
- Variables por nivel (reino, phylum, clase, orden…)  
- Descargas de datos  
- Compatibilidad con el estándar SPECIES v3.0  

### Funciones principales del controlador:

---

### **1. `get_variables(req, res)`**  
Obtiene el catálogo de variables pertenecientes a la fuente SNIB.

**Uso:**  
- Consulta `url_catvar` definido en `config.js`  
- Devuelve lista de variables compatibles con SPECIES v3.0  

---

### **2. `get_secuencia(req, res)`**  
Devuelve la secuencia taxonómica disponible.  
Ejemplo: Reino → Phylum → Clase → Orden → Familia → Género → Especie.

---

### **3. `get_variables_by_id(req, res)`**  
Obtiene el conjunto de variables perteneciente a un ID específico.

---

### **4. `get_data(req, res)`**  
Devuelve datos geográficos y biológicos asociados a una variable.  
Se utiliza para análisis de nicho y cálculos posteriores.

**Incluye:**  
- Recuperación de datos crudos  
- Conversión a estructura estándar  
- Caching opcional con Redis  

---

### **5. `get_variables_all(req, res)`**  
Función auxiliar que devuelve *todas* las variables disponibles.  
Útil para clientes que requieren exploración inicial de la fuente.

---

## Rutas principales: `snib_router.js`

La API expone endpoints como:

| Ruta | Método | Descripción |
|------|--------|-------------|
| `/variables` | GET | Lista de variables del SNIB |
| `/variables/:id` | GET | Variables por ID |
| `/secuencia` | GET | Secuencia taxonómica |
| `/data` | POST | Solicitud de datos para análisis |
| `/variables/all` | GET | Todas las variables del sistema |

Ejemplo básico de ruta:

```js
router.get("/variables", controller.get_variables);
```

---

## Servidor principal: `server.js`

Responsable de:

- Crear instancia Express  
- Cargar middleware global (JSON, CORS, compresión, etc.)  
- Registrar rutas de SNIB  
- Iniciar el servidor en el puerto definido  

---

## Instalación

```
npm install
```

## Variables de entorno necesarias `.env`

```
PORT=8087
REDIS_HOST=localhost
REDIS_PORT=6379
SNIB_URL_CATVAR=http://localhost:XXXX
...
```

---

## Ejecución

```
npm start
```

---

## Notas importantes

- Este middleware está diseñado **para integrarse con Species v3.0**, no es un proyecto independiente.  
- Solo estandariza datos del SNIB, pero puede extenderse a otras fuentes.  
- Redis es opcional, pero recomendado para reducir latencia.

---

## Referencias

- Estándar SPECIES v3.0  
  https://github.com/chilam-lab/species_v3.0  
- Documentación de CONABIO / SNIB  

---

