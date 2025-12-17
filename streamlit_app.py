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
from snowflake.snowpark.context import get_active_session

# Importar módulos personalizados
from data_processor import DataProcessor
from database_manager import DatabaseManager


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

    # Información de sesión
    try:
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
    except Exception as e:
        st.warning(f"No se pudo obtener información detallada de la sesión: {str(e)}")

    st.header("📁 Carga de Archivo")

    with st.expander("Columnas esperadas en el archivo"):
        st.markdown("""
        El archivo CSV debe contener las siguientes columnas:
        - **IDRechazo**: Identificador del rechazo
        - **Caso**: Número o identificador del caso
        - **Responsable de Caso**: Persona responsable
        - **Valor homologación**: Valor de homologación

        *Nota: Los nombres de las columnas deben coincidir exactamente (sin distinguir mayúsculas/minúsculas)*

        **Formato aceptado:** CSV únicamente
        """)

    uploaded_file = st.file_uploader(
        "Selecciona el archivo CSV",
        accept_multiple_files=False,
        help="Sube un archivo CSV con los datos de rechazos. Solo se aceptan archivos en formato CSV."
    )

    if uploaded_file is not None:
        # Validar extensión del archivo
        if not uploaded_file.name.lower().endswith('.csv'):
            st.error(f"❌ **Formato de archivo no válido**")
            st.warning(f"El archivo **{uploaded_file.name}** no es un archivo CSV válido.")
            st.info("Por favor, sube un archivo con extensión **.csv**. No se aceptan archivos Excel (.xlsx, .xls) ni otros formatos.")
            return

        try:
            st.info(f"Archivo cargado: {uploaded_file.name}")

            with st.spinner("Leyendo archivo..."):
                processor = DataProcessor()
                df = processor.read_csv(uploaded_file)

            st.success(f"Archivo leído correctamente: {len(df)} registros encontrados")

            st.subheader("Vista Previa de Datos")
            st.dataframe(df.head(10), use_container_width=True)

            with st.spinner("Validando datos..."):
                is_valid, errors = processor.validate_data(df)

            if not is_valid:
                st.error("Errores encontrados en el archivo:")
                for error in errors:
                    st.error(f"- {error}")
            else:
                st.success("Validación exitosa")

                with st.spinner("Transformando datos..."):
                    df_transformed = processor.transform_for_database(df)

                if st.button("Actualizar Registros", type="primary"):

                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    try:
                        status_text.text("Conectando a Snowflake...")
                        progress_bar.progress(10)

                        db_manager = DatabaseManager(database=database, schema=schema)

                        status_text.text("Verificando tabla...")
                        progress_bar.progress(20)

                        if not db_manager.verify_table_exists():
                            st.error("La tabla RECHAZOS_SEGUIMIENTO no existe")
                            return

                        status_text.text("Actualizando registros...")
                        progress_bar.progress(40)

                        results = db_manager.update_rechazos(df_transformed)

                        progress_bar.progress(70)

                        # Procesar homologaciones
                        homologacion_results = None
                        sucursal_results = None
                        if results['updated_ids']:
                            status_text.text("Procesando homologaciones de productos...")
                            homologacion_results = db_manager.insert_homologaciones_from_rechazos(results['updated_ids'])

                            status_text.text("Procesando homologaciones de sucursales...")
                            sucursal_results = db_manager.insert_homologaciones_sucursales_from_rechazos(results['updated_ids'])

                        progress_bar.progress(100)
                        status_text.text("Proceso completado")

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
                        st.error(f"Error durante la actualización: {str(e)}")
                        st.code(traceback.format_exc())

        except Exception as e:
            st.error(f"Error al procesar el archivo: {str(e)}")
            st.code(traceback.format_exc())

if __name__ == "__main__":
    main()
