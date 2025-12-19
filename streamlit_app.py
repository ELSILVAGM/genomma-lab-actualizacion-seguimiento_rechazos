"""
=============================================================================
APLICACIÓN DE ACTUALIZACIÓN DE RECHAZOS - STREAMLIT IN SNOWFLAKE
=============================================================================
Esta aplicación permite cargar un archivo CSV con datos de rechazos y
actualizarlos en la tabla RECHAZOS_SEGUIMIENTO en Snowflake.
Además, inserta automáticamente homologaciones en PRO_SO_HOMOLOGACION.
=============================================================================
"""

import streamlit as st
import pandas as pd
import traceback
import logging
from datetime import datetime
from snowflake.snowpark.context import get_active_session

# Importar módulos personalizados
from data_processor import DataProcessor
from database_manager import DatabaseManager

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar session state para logs
if 'logs' not in st.session_state:
    st.session_state.logs = []

def add_log(message, level="INFO"):
    """Agregar mensaje al log"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{level}] {message}"
    st.session_state.logs.append(log_entry)
    logger.info(log_entry)


# =============================================================================
# CONFIGURACIÓN DE LA APLICACIÓN
# =============================================================================

st.set_page_config(
    page_title="Actualización de Rechazos",
    page_icon="📁",
    layout="wide"
)


# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def main():
    # Obtener el prefijo de base de datos de la sesión actual (DEV_STG o PRD_STG)
    try:
        session = get_active_session()
        session_info = session.sql("SELECT CURRENT_DATABASE() as DATABASE").collect()
        current_database = session_info[0]['DATABASE']  # Ej: DEV_STG o PRD_STG

        # Extraer el prefijo (DEV_ o PRD_)
        if current_database.startswith('DEV_'):
            database_prefix = 'DEV_'
        elif current_database.startswith('PRD_'):
            database_prefix = 'PRD_'
        else:
            database_prefix = 'DEV_'  # Default

        # Construir nombres de bases de datos
        database = f"{database_prefix}STG"
        schema = "GNM_CT"  # Schema fijo

    except Exception as e:
        st.error(f"No se pudo obtener información de la sesión: {str(e)}")
        return

    st.title("Actualización de seguimiento de rechazos")
    st.markdown(f"""
    Esta aplicación permite cargar un archivo CSV con datos de rechazos y
    actualizarlos en la tabla de seguimiento de rechazos en Snowflake.

    **Entorno actual:** `{database}` ({database_prefix.rstrip('_')})
    """)

    # Agregar pestaña de logs
    tab1, tab2 = st.tabs(["📁 Carga de Archivo", "📋 Logs de Depuración"])

    with tab2:
        st.subheader("Logs de Depuración")
        if st.button("🗑️ Limpiar Logs"):
            st.session_state.logs = []
            st.rerun()

        if st.session_state.logs:
            st.text_area(
                "Mensajes de log:",
                value="\n".join(st.session_state.logs),
                height=400,
                disabled=True
            )
        else:
            st.info("No hay logs aún. Los logs aparecerán cuando uses la aplicación.")

    with tab1:
        # Información de sesión
        try:
            add_log("Obteniendo información de la sesión...")
            session_info_full = session.sql("SELECT CURRENT_USER() as USUARIO, CURRENT_DATABASE() as DATABASE, CURRENT_SCHEMA() as SCHEMA, CURRENT_ROLE() as ROLE, CURRENT_WAREHOUSE() as WAREHOUSE").collect()

            with st.expander("Información de la sesión actual"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Usuario", session_info_full[0]['USUARIO'])
                    st.metric("Database", session_info_full[0]['DATABASE'])
                with col2:
                    st.metric("Schema", session_info_full[0]['SCHEMA'])
                    st.metric("Role", session_info_full[0]['ROLE'])
                with col3:
                    st.metric("Warehouse", session_info_full[0]['WAREHOUSE'])
            add_log(f"Sesión obtenida - Database: {session_info_full[0]['DATABASE']}, Schema: {session_info_full[0]['SCHEMA']}, Role: {session_info_full[0]['ROLE']}")
        except Exception as e:
            add_log(f"Error obteniendo información de sesión: {str(e)}", "ERROR")
            st.warning(f"No se pudo obtener información detallada de la sesión: {str(e)}")

        st.header("📁 Carga de Archivo")

        with st.expander("Columnas esperadas en el archivo"):
            st.markdown("""
            El archivo debe contener las siguientes columnas:

            | Campo | Definición |
            |-------|------------|
            | **IDRechazo** | Identificador único del rechazo |
            | **Caso** | Caso que indica la solución que se debe dar al rechazo |
            | **Responsable de Caso** | Área del equipo de Ciencia de Datos que debe dar seguimiento al rechazo |
            | **Valor Homologacion** | Valor al que se debe homologar si el campo "Caso" corresponde a: Homologacion Sucursal o Homologacion Producto |

            *Nota: Los nombres de las columnas deben coincidir exactamente (sin distinguir mayúsculas/minúsculas)*

            **Formatos aceptados:** CSV, XLSX
            """)

        uploaded_file = st.file_uploader(
            "Selecciona el archivo (CSV o XLSX)",
            accept_multiple_files=False,
            help="Sube un archivo CSV o XLSX con los datos de rechazos."
        )

        if uploaded_file is not None:
            add_log(f"Archivo seleccionado: {uploaded_file.name}")
            # Validar extensión del archivo
            if not (uploaded_file.name.lower().endswith('.csv') or uploaded_file.name.lower().endswith('.xlsx')):
                add_log(f"Archivo rechazado - extensión inválida: {uploaded_file.name}", "WARNING")
                st.error(f"❌ **Formato de archivo no válido**")
                st.warning(f"El archivo **{uploaded_file.name}** no tiene un formato válido.")
                st.info("Por favor, sube un archivo con extensión **.csv** o **.xlsx**. No se aceptan otros formatos.")
                return

            try:
                st.info(f"Archivo cargado: {uploaded_file.name}")
                add_log(f"Iniciando lectura del archivo {uploaded_file.name}...")

                with st.spinner("Leyendo archivo..."):
                    processor = DataProcessor()
                    df = processor.read_file(uploaded_file)

                add_log(f"Archivo leído correctamente: {len(df)} registros encontrados")
                st.success(f"Archivo leído correctamente: {len(df)} registros encontrados")

                st.subheader("Vista Previa de Datos")
                st.dataframe(df.head(10), use_container_width=True)

                add_log("Validando datos...")
                with st.spinner("Validando datos..."):
                    is_valid, errors = processor.validate_data(df)

                if not is_valid:
                    add_log(f"Validación fallida - {len(errors)} errores encontrados", "ERROR")
                    st.error("Errores encontrados en el archivo:")
                    for error in errors:
                        add_log(f"Error de validación: {error}", "ERROR")
                        st.error(f"- {error}")
                else:
                    add_log("Validación exitosa")
                    st.success("Validación exitosa")

                    add_log("Transformando datos...")
                    with st.spinner("Transformando datos..."):
                        df_transformed = processor.transform_for_database(df)

                    if st.button("Actualizar Registros", type="primary"):
                        add_log("Iniciando actualización de registros...")

                        progress_bar = st.progress(0)
                        status_text = st.empty()

                        try:
                            add_log("Conectando a Snowflake...")
                            status_text.text("Conectando a Snowflake...")
                            progress_bar.progress(10)

                            db_manager = DatabaseManager(database=database, schema=schema)

                            add_log("Verificando existencia de tabla...")
                            status_text.text("Verificando tabla...")
                            progress_bar.progress(20)

                            if not db_manager.verify_table_exists():
                                add_log("Error: Tabla RECHAZOS_SEGUIMIENTO no existe", "ERROR")
                                st.error("La tabla RECHAZOS_SEGUIMIENTO no existe")
                                return

                            add_log("Iniciando actualización de rechazos...")
                            status_text.text("Actualizando registros...")
                            progress_bar.progress(40)

                            results = db_manager.update_rechazos(df_transformed)
                            add_log(f"Actualización completada - {results['updated']} registros actualizados, {results['failed']} fallidos")

                            progress_bar.progress(70)

                            # Procesar homologaciones
                            homologacion_results = None
                            sucursal_results = None
                            if results['updated_ids']:
                                add_log(f"Procesando homologaciones para {len(results['updated_ids'])} registros...")
                                status_text.text("Procesando homologaciones de productos...")
                                homologacion_results = db_manager.insert_homologaciones_from_rechazos(results['updated_ids'])
                                add_log(f"Homologaciones de productos - {homologacion_results['inserted']} insertadas, {homologacion_results['duplicated']} duplicadas")

                                status_text.text("Procesando homologaciones de sucursales...")
                                sucursal_results = db_manager.insert_homologaciones_sucursales_from_rechazos(results['updated_ids'])
                                add_log(f"Homologaciones de sucursales - {sucursal_results['inserted']} insertadas, {sucursal_results['duplicated']} duplicadas")

                            progress_bar.progress(100)
                            status_text.text("Proceso completado")
                            add_log("✅ Proceso completado exitosamente")

                            # Resultados de actualización
                            st.success(f"Proceso completado: {results['updated']} registros actualizados correctamente")

                            if results['failed'] > 0:
                                st.warning(f"Registros fallidos: {results['failed']}")

                            if results['errors']:
                                with st.expander("Ver errores de actualización"):
                                    for error in results['errors']:
                                        st.error(error)

                            # Resultados de homologaciones de productos
                            if homologacion_results and homologacion_results['total'] > 0:
                                st.subheader("Homologaciones de Productos")

                                if homologacion_results['inserted'] > 0:
                                    st.success(f"Se insertaron {homologacion_results['inserted']} homologaciones nuevas")
                                    if homologacion_results.get('inserted_details'):
                                        with st.expander("Ver homologaciones insertadas"):
                                            df_insertadas = pd.DataFrame(homologacion_results['inserted_details'])
                                            st.dataframe(df_insertadas, use_container_width=True)

                                if homologacion_results['duplicated'] > 0:
                                    st.warning(f"Se encontraron {homologacion_results['duplicated']} homologaciones duplicadas (no se insertaron)")
                                    if homologacion_results['duplicates']:
                                        with st.expander("Ver homologaciones duplicadas"):
                                            df_duplicados = pd.DataFrame(homologacion_results['duplicates'])
                                            st.dataframe(df_duplicados, use_container_width=True)

                                if homologacion_results['failed'] > 0:
                                    st.error(f"Fallaron {homologacion_results['failed']} homologaciones")

                                if homologacion_results['errors']:
                                    with st.expander("Ver errores de homologaciones"):
                                        for error in homologacion_results['errors']:
                                            st.error(error)

                            # Resultados de homologaciones de sucursales (solo si hay resultados)
                            if sucursal_results and sucursal_results['total'] > 0:
                                st.subheader("Homologaciones de Sucursales")

                                if sucursal_results['inserted'] > 0:
                                    st.success(f"Se insertaron {sucursal_results['inserted']} homologaciones de sucursales nuevas")
                                    if sucursal_results.get('inserted_details'):
                                        with st.expander("Ver homologaciones de sucursales insertadas"):
                                            df_suc_insertadas = pd.DataFrame(sucursal_results['inserted_details'])
                                            st.dataframe(df_suc_insertadas, use_container_width=True)

                                if sucursal_results['duplicated'] > 0:
                                    st.warning(f"Se encontraron {sucursal_results['duplicated']} homologaciones de sucursales duplicadas (no se insertaron)")
                                    if sucursal_results['duplicates']:
                                        with st.expander("Ver homologaciones de sucursales duplicadas"):
                                            df_suc_duplicados = pd.DataFrame(sucursal_results['duplicates'])
                                            st.dataframe(df_suc_duplicados, use_container_width=True)

                                if sucursal_results['failed'] > 0:
                                    st.error(f"Fallaron {sucursal_results['failed']} homologaciones de sucursales")

                                if sucursal_results['errors']:
                                    with st.expander("Ver detalles de errores de homologaciones de sucursales"):
                                        for error in sucursal_results['errors']:
                                            st.error(error)

                            db_manager.close()

                        except Exception as e:
                            add_log(f"ERROR CRÍTICO durante actualización: {str(e)}", "ERROR")
                            add_log(f"Traceback: {traceback.format_exc()}", "ERROR")
                            st.error(f"Error durante la actualización: {str(e)}")
                            st.code(traceback.format_exc())

            except Exception as e:
                add_log(f"ERROR al procesar el archivo: {str(e)}", "ERROR")
                add_log(f"Traceback: {traceback.format_exc()}", "ERROR")
                st.error(f"Error al procesar el archivo: {str(e)}")
                st.code(traceback.format_exc())

if __name__ == "__main__":
    main()
