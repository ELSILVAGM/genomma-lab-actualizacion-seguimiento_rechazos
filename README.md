# Sistema de Actualización de Rechazos - Streamlit in Snowflake

Aplicación web desarrollada con Streamlit para la actualización masiva de rechazos y generación automática de homologaciones en Snowflake.

## 📋 Descripción

Esta aplicación permite:
- ✅ Cargar archivos CSV con datos de rechazos
- ✅ Actualizar registros en la tabla `RECHAZOS_SEGUIMIENTO`
- ✅ Generar automáticamente homologaciones de productos en `PRO_SO_HOMOLOGACION`
- ✅ Generar automáticamente homologaciones de sucursales en `SUC_SO_HOMOLOGACION`
- ✅ Adaptarse dinámicamente a entornos DEV y PRD
- ✅ Validación robusta de datos con mensajes de error claros

## 🏗️ Arquitectura

La aplicación está diseñada con una arquitectura modular:

```
github_version/
├── streamlit_app.py       # Aplicación principal (punto de entrada)
├── data_processor.py      # Procesamiento y validación de datos
├── database_manager.py    # Operaciones con Snowflake
├── environment.yml        # Dependencias del entorno
└── README.md             # Esta documentación
```

### Módulos

- **streamlit_app.py**: Interfaz de usuario y flujo principal de la aplicación
- **data_processor.py**: Clase `DataProcessor` para lectura, validación y transformación de archivos CSV
- **database_manager.py**: Clase `DatabaseManager` para todas las operaciones con Snowflake usando Snowpark

## 🚀 Despliegue en Snowflake

### Opción 1: Desde Repositorio de GitHub

1. **Subir el código a GitHub:**
   ```bash
   git init
   git add .
   git commit -m "Initial commit - Sistema de Rechazos"
   git branch -M main
   git remote add origin https://github.com/TU_USUARIO/TU_REPO.git
   git push -u origin main
   ```

2. **Crear la aplicación en Snowflake:**
   ```sql
   CREATE STREAMLIT sistema_rechazos
   ROOT_LOCATION = '@database.schema.stage'
   MAIN_FILE = '/streamlit_app.py'
   QUERY_WAREHOUSE = 'TU_WAREHOUSE'
   COMMENT = 'Sistema de actualización de rechazos y homologaciones';
   ```

3. **Configurar el repositorio de GitHub en Snowflake:**
   ```sql
   -- Crear integración con GitHub (si no existe)
   CREATE OR REPLACE API INTEGRATION git_api_integration
   API_PROVIDER = git_https_api
   API_ALLOWED_PREFIXES = ('https://github.com/TU_USUARIO/')
   ENABLED = TRUE;

   -- Crear repositorio Git
   CREATE OR REPLACE GIT REPOSITORY sistema_rechazos_repo
   API_INTEGRATION = git_api_integration
   ORIGIN = 'https://github.com/TU_USUARIO/TU_REPO.git';

   -- Crear aplicación desde el repositorio
   CREATE STREAMLIT sistema_rechazos
   ROOT_LOCATION = '@sistema_rechazos_repo/branches/main/'
   MAIN_FILE = 'streamlit_app.py'
   QUERY_WAREHOUSE = 'TU_WAREHOUSE';
   ```

### Opción 2: Carga Manual desde Snowsight

1. Ve a **Snowsight** → **Streamlit**
2. Haz clic en **+ Streamlit App**
3. Selecciona **Create from GitHub repository** o **Upload files**
4. Configura:
   - **Name**: `sistema_rechazos`
   - **Warehouse**: Selecciona tu warehouse
   - **Database** y **Schema**: Donde quieres crear la app
5. Sube los archivos:
   - `streamlit_app.py`
   - `data_processor.py`
   - `database_manager.py`
   - `environment.yml`

## 📊 Requisitos de Permisos

La aplicación necesita los siguientes permisos en Snowflake:

```sql
-- Permisos de lectura
GRANT USAGE ON DATABASE DEV_STG TO ROLE TU_ROLE;
GRANT USAGE ON SCHEMA DEV_STG.GNM_CT TO ROLE TU_ROLE;
GRANT USAGE ON SCHEMA DEV_STG.GNM_CF TO ROLE TU_ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA DEV_STG.GNM_CT TO ROLE TU_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA DEV_STG.GNM_CT TO ROLE TU_ROLE;
GRANT SELECT ON TABLE DEV_STG.GNM_CF.CF_CLIENTES_SO TO ROLE TU_ROLE;

-- Permisos de escritura
GRANT UPDATE ON TABLE DEV_STG.GNM_CT.RECHAZOS_SEGUIMIENTO TO ROLE TU_ROLE;
GRANT INSERT ON TABLE DEV_STG.GNM_CT.PRO_SO_HOMOLOGACION TO ROLE TU_ROLE;
GRANT INSERT ON TABLE DEV_STG.GNM_CT.SUC_SO_HOMOLOGACION TO ROLE TU_ROLE;

-- Permisos de warehouse
GRANT USAGE ON WAREHOUSE TU_WAREHOUSE TO ROLE TU_ROLE;
```

## 📁 Formato del Archivo CSV

El archivo CSV debe contener las siguientes columnas:

| Columna | Descripción | Requerido |
|---------|-------------|-----------|
| **IDRechazo** | Identificador único del rechazo | ✅ Sí |
| **Caso** | Número o identificador del caso | ⚠️ Al menos una |
| **Responsable de Caso** | Persona responsable del caso | ⚠️ Al menos una |
| **Valor homologación** | Valor de homologación a aplicar | ⚠️ Al menos una |

**Notas:**
- Los nombres de las columnas NO distinguen mayúsculas/minúsculas
- Al menos una de las columnas `Caso`, `Responsable de Caso` o `Valor homologación` debe tener datos
- El archivo debe estar en formato CSV con codificación UTF-8, Latin-1 o Windows-1252

### Ejemplo de archivo CSV:

```csv
IDRechazo,Caso,Responsable de Caso,Valor homologación
12345,CASO-001,Gobierno de Datos,PROD123
12346,CASO-002,Gobierno de Datos,PROD456
12347,CASO-003,Gobierno de Datos,SUC789
```

## 🔄 Flujo de Trabajo

1. **Carga del archivo**: El usuario sube un archivo CSV
2. **Validación**: Se valida la estructura y contenido del archivo
3. **Transformación**: Los datos se transforman al formato de base de datos
4. **Actualización**: Se actualizan los registros en `RECHAZOS_SEGUIMIENTO`
5. **Homologaciones automáticas**:
   - Si el rechazo es de tipo `PROPSTID`, se inserta en `PRO_SO_HOMOLOGACION`
   - Si el rechazo es de tipo `SUCID`, se inserta en `SUC_SO_HOMOLOGACION`
6. **Resultados**: Se muestran estadísticas detalladas de la operación

## 🌍 Entornos

La aplicación se adapta automáticamente al entorno:

- **DEV**: Usa `DEV_STG.GNM_CT` y `DEV_STG.GNM_CF`
- **PRD**: Usa `PRD_STG.GNM_CT` y `PRD_STG.GNM_CF`

El entorno se detecta automáticamente desde `CURRENT_DATABASE()`.

## 🔐 Seguridad

- ✅ Protección contra SQL Injection (todos los strings se escapan correctamente)
- ✅ Validación de extensiones de archivo (solo CSV)
- ✅ Validación de permisos en tiempo de ejecución
- ✅ Uso de Snowpark Session (sin credenciales hardcodeadas)

## 🛠️ Tecnologías

- **Streamlit**: Framework de interfaz de usuario
- **Snowpark Python**: SDK de Snowflake para Python
- **Pandas**: Procesamiento de datos
- **Python 3.9+**: Lenguaje de programación

## 📝 Mantenimiento

### Actualizar desde GitHub

Si estás usando la integración con GitHub:

```sql
-- Actualizar el repositorio
ALTER GIT REPOSITORY sistema_rechazos_repo FETCH;

-- Refrescar la aplicación
ALTER STREAMLIT sistema_rechazos SET ROOT_LOCATION = '@sistema_rechazos_repo/branches/main/';
```

### Monitoreo

Para ver logs de la aplicación:
- Ve a **Snowsight** → **Streamlit** → **sistema_rechazos**
- Haz clic en **Logs** en la parte superior derecha

## 🤝 Contribuciones

Para contribuir al proyecto:

1. Haz un fork del repositorio
2. Crea una rama para tu feature (`git checkout -b feature/nueva-funcionalidad`)
3. Haz commit de tus cambios (`git commit -m 'Agregar nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Abre un Pull Request

## 📄 Licencia

Este proyecto es de uso interno de la organización.

## 📞 Soporte

Para reportar problemas o solicitar nuevas funcionalidades, contacta al equipo de Gobierno de Datos.

---

**Versión**: 2.0 (Modular)
**Última actualización**: Diciembre 2024
