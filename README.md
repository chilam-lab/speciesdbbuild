
# Proyecto middleware_speciesv3 

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

