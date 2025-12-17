"""
=============================================================================
DATA PROCESSOR - Procesamiento y validación de datos
=============================================================================
Módulo para procesar y validar archivos CSV de rechazos
=============================================================================
"""

import pandas as pd
from typing import Tuple, List
from datetime import datetime


class DataProcessor:
    """Clase para procesar y validar datos del archivo CSV"""

    COLUMN_MAPPING = {
        'IDRechazo': 'RECHAZOID',
        'Caso': 'CASO',
        'Responsable de Caso': 'RESPONSABLE_DE_CASO',
        'Valor homologación': 'VALOR_HOMOLOGACION'
    }

    REQUIRED_COLUMNS = ['IDRechazo', 'Caso', 'Responsable de Caso', 'Valor homologación']

    def __init__(self):
        pass

    def read_csv(self, file) -> pd.DataFrame:
        """Lee archivos CSV con manejo robusto de codificaciones"""
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'windows-1252', 'cp1252']

        for encoding in encodings:
            try:
                if hasattr(file, 'seek'):
                    file.seek(0)

                df = pd.read_csv(file, encoding=encoding)
                df.columns = df.columns.str.strip()
                return df

            except (UnicodeDecodeError, UnicodeError):
                continue
            except Exception as e:
                if encoding == encodings[-1]:  # Si es el último intento
                    raise Exception(f"Error al leer el archivo CSV: {str(e)}")

        raise Exception(
            f"No se pudo leer el archivo CSV. Se probaron las siguientes codificaciones: {', '.join(encodings)}. "
            "Por favor, asegúrate de que el archivo esté en un formato CSV válido."
        )

    def validate_data(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        errors = []

        # Verificar columnas requeridas
        missing_columns = []
        for col in self.REQUIRED_COLUMNS:
            found = False
            for df_col in df.columns:
                if df_col.lower().strip() == col.lower().strip():
                    found = True
                    break
            if not found:
                missing_columns.append(col)

        if missing_columns:
            errors.append(f"Columnas faltantes: {', '.join(missing_columns)}")
            return False, errors

        df_normalized = df.copy()
        df_normalized.columns = [col.lower().strip() for col in df.columns]

        # Verificar IDRechazo
        id_col = self._find_column(df_normalized, 'idrechazo')
        if id_col:
            null_ids = df_normalized[id_col].isnull().sum()
            if null_ids > 0:
                errors.append(f"Se encontraron {null_ids} registros sin IDRechazo")

            duplicated_ids = df_normalized[id_col].dropna().duplicated()
            if duplicated_ids.any():
                duplicate_count = duplicated_ids.sum()
                duplicate_values = df_normalized[id_col][df_normalized[id_col].duplicated(keep=False)].unique()
                errors.append(
                    f"Se encontraron {duplicate_count} IDRechazo duplicados en el archivo. "
                    f"IDs duplicados: {', '.join(map(str, duplicate_values[:10]))}"
                    f"{' ...' if len(duplicate_values) > 10 else ''}"
                )

            try:
                pd.to_numeric(df_normalized[id_col].dropna(), errors='raise')
            except:
                errors.append("IDRechazo contiene valores no numéricos")

        # Validar que haya datos para actualizar
        caso_col = self._find_column(df_normalized, 'caso')
        resp_col = self._find_column(df_normalized, 'responsable de caso')
        valor_col = self._find_column(df_normalized, 'valor homologación')

        has_update_data = False
        if caso_col and not df_normalized[caso_col].isnull().all():
            has_update_data = True
        if resp_col and not df_normalized[resp_col].isnull().all():
            has_update_data = True
        if valor_col and not df_normalized[valor_col].isnull().all():
            has_update_data = True

        if not has_update_data:
            errors.append("No hay datos para actualizar (todas las columnas de actualización están vacías)")

        return len(errors) == 0, errors

    def transform_for_database(self, df: pd.DataFrame) -> pd.DataFrame:
        df_transformed = df.copy()

        column_mapping = {}
        for excel_col, db_col in self.COLUMN_MAPPING.items():
            for col in df_transformed.columns:
                if col.lower().strip() == excel_col.lower().strip():
                    column_mapping[col] = db_col
                    break

        df_transformed = df_transformed.rename(columns=column_mapping)
        df_transformed['UPDATE_AT'] = datetime.now()
        df_transformed['FECHA_SOLUCION_RECHAZO'] = datetime.now()

        if 'RECHAZOID' in df_transformed.columns:
            df_transformed['RECHAZOID'] = pd.to_numeric(df_transformed['RECHAZOID'], errors='coerce')

        string_columns = ['CASO', 'RESPONSABLE_DE_CASO', 'VALOR_HOMOLOGACION']
        for col in string_columns:
            if col in df_transformed.columns:
                df_transformed[col] = df_transformed[col].astype(str).str.strip()
                df_transformed[col] = df_transformed[col].replace('nan', None)

        available_columns = ['RECHAZOID', 'CASO', 'RESPONSABLE_DE_CASO',
                           'VALOR_HOMOLOGACION', 'UPDATE_AT', 'FECHA_SOLUCION_RECHAZO']
        df_transformed = df_transformed[[col for col in available_columns if col in df_transformed.columns]]
        df_transformed = df_transformed.dropna(subset=['RECHAZOID'])

        return df_transformed

    def _find_column(self, df: pd.DataFrame, column_name: str) -> str:
        column_name_lower = column_name.lower().strip()
        for col in df.columns:
            if col.lower().strip() == column_name_lower:
                return col
        return None
