"""
=============================================================================
DATABASE MANAGER - Gestión de operaciones con Snowflake
=============================================================================
Módulo para manejar todas las operaciones de base de datos usando Snowpark
=============================================================================
"""

import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
from snowflake.snowpark.context import get_active_session


class DatabaseManager:
    """Clase para manejar operaciones con Snowflake usando Snowpark"""

    def __init__(self, database: str = "DEV_STG", schema: str = "GNM_CT"):
        self.database = database
        self.schema = schema
        self.session = get_active_session()

    def update_rechazos(self, df: pd.DataFrame) -> Dict[str, Any]:
        results = {
            'total': len(df),
            'updated': 0,
            'failed': 0,
            'errors': [],
            'updated_ids': []
        }

        table_name = f"{self.database}.{self.schema}.RECHAZOS_SEGUIMIENTO"

        for idx, row in df.iterrows():
            try:
                update_at = row.get('UPDATE_AT', datetime.now())
                if isinstance(update_at, datetime):
                    update_at = update_at.strftime('%Y-%m-%d %H:%M:%S')

                fecha_solucion = row.get('FECHA_SOLUCION_RECHAZO', datetime.now())
                if isinstance(fecha_solucion, datetime):
                    fecha_solucion = fecha_solucion.strftime('%Y-%m-%d %H:%M:%S')

                rechazoid = int(row['RECHAZOID'])

                set_clauses = [
                    f"UPDATE_AT = '{update_at}'",
                    f"FECHA_SOLUCION_RECHAZO = '{fecha_solucion}'"
                ]

                if 'CASO' in row and pd.notna(row['CASO']):
                    caso_val = str(row['CASO']).replace("'", "''")
                    set_clauses.append(f"CASO = '{caso_val}'")

                if 'RESPONSABLE_DE_CASO' in row and pd.notna(row['RESPONSABLE_DE_CASO']):
                    resp_val = str(row['RESPONSABLE_DE_CASO']).replace("'", "''")
                    set_clauses.append(f"RESPONSABLE_DE_CASO = '{resp_val}'")

                if 'VALOR_HOMOLOGACION' in row and pd.notna(row['VALOR_HOMOLOGACION']):
                    valor_val = str(row['VALOR_HOMOLOGACION']).replace("'", "''")
                    set_clauses.append(f"VALOR_HOMOLOGACION = '{valor_val}'")

                query = f"""
                UPDATE {table_name}
                SET {', '.join(set_clauses)}
                WHERE RECHAZOID = {rechazoid}
                """

                self.session.sql(query).collect()

                results['updated'] += 1
                results['updated_ids'].append(rechazoid)

                # Lógica de compartir EAN
                if 'VALOR_HOMOLOGACION' in row and pd.notna(row['VALOR_HOMOLOGACION']):
                    info_query = f"""
                    SELECT CAMPO_RECHAZADO, PAISID, CODIGO_BARRAS
                    FROM {table_name}
                    WHERE RECHAZOID = {rechazoid}
                    """
                    info_df = self.session.sql(info_query).to_pandas()

                    if len(info_df) > 0 and info_df.iloc[0]['CAMPO_RECHAZADO'] == 'PROPSTID':
                        paisid = info_df.iloc[0]['PAISID']
                        codigo_barras = str(info_df.iloc[0]['CODIGO_BARRAS']).replace("'", "''")
                        valor_homologacion = str(row['VALOR_HOMOLOGACION']).replace("'", "''")

                        update_compartido_query = f"""
                        UPDATE {table_name}
                        SET VALOR_HOMOLOGACION = '{valor_homologacion}',
                            UPDATE_AT = '{update_at}',
                            FECHA_SOLUCION_RECHAZO = '{fecha_solucion}'
                        WHERE RECHAZOID != {rechazoid}
                            AND PAISID = {paisid}
                            AND CODIGO_BARRAS = '{codigo_barras}'
                            AND CAMPO_RECHAZADO = 'PROPSTID'
                            AND GRPID IN (
                                SELECT GRPID
                                FROM {self.database}.GNM_CF.CF_CLIENTES_SO
                                WHERE COMPARTE_EAN = TRUE
                            )
                        """

                        self.session.sql(update_compartido_query).collect()

                        ids_compartidos_query = f"""
                        SELECT RECHAZOID
                        FROM {table_name}
                        WHERE RECHAZOID != {rechazoid}
                            AND PAISID = {paisid}
                            AND CODIGO_BARRAS = '{codigo_barras}'
                            AND CAMPO_RECHAZADO = 'PROPSTID'
                            AND GRPID IN (
                                SELECT GRPID
                                FROM {self.database}.GNM_CF.CF_CLIENTES_SO
                                WHERE COMPARTE_EAN = TRUE
                            )
                        """
                        ids_compartidos_df = self.session.sql(ids_compartidos_query).to_pandas()

                        for _, id_row in ids_compartidos_df.iterrows():
                            id_compartido = int(id_row['RECHAZOID'])
                            if id_compartido not in results['updated_ids']:
                                results['updated_ids'].append(id_compartido)
                                results['updated'] += 1

            except Exception as e:
                results['failed'] += 1
                error_msg = f"Registro {idx+1} (ID: {row.get('RECHAZOID', 'N/A')}): {str(e)}"
                results['errors'].append(error_msg)

        return results

    def insert_homologaciones_from_rechazos(self, rechazos_ids: List[int]) -> Dict[str, Any]:
        results = {
            'total': 0,
            'inserted': 0,
            'duplicated': 0,
            'failed': 0,
            'errors': [],
            'duplicates': [],
            'inserted_details': []
        }

        if not rechazos_ids:
            return results

        try:
            ids_str = ','.join(map(str, rechazos_ids))

            query_rechazos = f"""
            SELECT
                r.RECHAZOID,
                r.PAISID,
                r.VALOR_RECHAZADO as COD_PROD,
                r.GRPID,
                r.VALOR_HOMOLOGACION as PROPSTID,
                r.CODIGO_BARRAS as PROPSTCODBARRAS,
                r.SEMANAS
            FROM {self.database}.{self.schema}.RECHAZOS_SEGUIMIENTO r
            WHERE r.RECHAZOID IN ({ids_str})
                AND r.RESPONSABLE_DE_CASO = 'Gobierno de Datos'
                AND r.MODULO = 'Sellout'
                AND r.CAMPO_RECHAZADO = 'PROPSTID'
                AND r.MOTIVO_RECHAZO IN ('Producto no encontrado en tabla de homologación')
            """

            rechazos_df = self.session.sql(query_rechazos).to_pandas()
            results['total'] = len(rechazos_df)

            if results['total'] == 0:
                return results

            # Obtener descripciones de productos
            try:
                propstids = rechazos_df['PROPSTID'].unique().tolist()
                propstids_str = ','.join([f"'{x}'" for x in propstids if pd.notna(x)])

                if propstids_str:
                    desc_query = f"""
                    SELECT PROPSTID, PROPSTNOMBRE
                    FROM {self.database}.{self.schema}.VW_ESTRUCTURAPRODUCTOSTOTALPAISES
                    WHERE PROPSTID IN ({propstids_str})
                    """
                    desc_df = self.session.sql(desc_query).to_pandas()
                    desc_dict = dict(zip(desc_df['PROPSTID'], desc_df['PROPSTNOMBRE']))
                    rechazos_df['DESCRIPCION_PRODUCTO'] = rechazos_df['PROPSTID'].map(desc_dict).fillna('Producto homologado')
                else:
                    rechazos_df['DESCRIPCION_PRODUCTO'] = 'Producto homologado'
            except Exception as e:
                rechazos_df['DESCRIPCION_PRODUCTO'] = 'Producto homologado'

            for idx, row in rechazos_df.iterrows():
                rechazoid = row['RECHAZOID']
                paisid = row['PAISID']
                cod_prod = str(row['COD_PROD']).replace("'", "''") if pd.notna(row['COD_PROD']) else ''
                grpid = row['GRPID']
                propstid = str(row['PROPSTID']).replace("'", "''") if pd.notna(row['PROPSTID']) else ''
                propstcodbarras = str(row['PROPSTCODBARRAS']).replace("'", "''") if pd.notna(row['PROPSTCODBARRAS']) else None
                semanas = row['SEMANAS']
                descripcion_producto = str(row['DESCRIPCION_PRODUCTO']).replace("'", "''") if pd.notna(row['DESCRIPCION_PRODUCTO']) else 'Producto homologado'

                try:
                    # Verificar duplicados
                    check_query = f"""
                    SELECT COUNT(*)
                    FROM {self.database}.{self.schema}.PRO_SO_HOMOLOGACION
                    WHERE PAISID = {paisid}
                        AND COD_PROD = '{cod_prod}'
                        AND GRPID = {grpid}
                    """

                    count_df = self.session.sql(check_query).to_pandas()
                    count = count_df.iloc[0, 0]

                    if count > 0:
                        results['duplicated'] += 1
                        results['duplicates'].append({
                            'RECHAZOID': rechazoid,
                            'PAISID': paisid,
                            'COD_PROD': cod_prod,
                            'GRPID': grpid,
                            'PROPSTID': propstid
                        })
                        continue

                    # Obtener FECHA_VALIDO_DESDE
                    fecha_valido_desde = None
                    if pd.notna(semanas):
                        try:
                            semanio = int(semanas) // 100
                            semnumero = int(semanas) % 100

                            semanas_query = f"""
                            SELECT SEMINICIO
                            FROM {self.database}.{self.schema}.CATSEMANAS
                            WHERE SEMANIO = {semanio}
                                AND SEMNUMERO = {semnumero}
                            """

                            semanas_df = self.session.sql(semanas_query).to_pandas()
                            if len(semanas_df) > 0:
                                fecha_valido_desde = semanas_df.iloc[0, 0]
                                if isinstance(fecha_valido_desde, datetime):
                                    fecha_valido_desde = fecha_valido_desde.strftime('%Y-%m-%d %H:%M:%S')
                        except Exception as e:
                            pass

                    fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    fecha_valido_hasta = '2999-12-31 00:00:00'

                    insert_query = f"""
                    INSERT INTO {self.database}.{self.schema}.PRO_SO_HOMOLOGACION (
                        PAISID,
                        COD_PROD,
                        DESCRIPCION_PRODUCTO,
                        GRPID,
                        PROPSTID,
                        PROPSTCODBARRAS,
                        ACTIVO,
                        CREATE_AT,
                        UPDATE_AT,
                        FECHA_VALIDO_DESDE,
                        FECHA_VALIDO_HASTA
                    ) VALUES (
                        {paisid},
                        '{cod_prod}',
                        '{descripcion_producto}',
                        {grpid},
                        '{propstid}',
                        {f"'{propstcodbarras}'" if propstcodbarras is not None else 'NULL'},
                        TRUE,
                        '{fecha_actual}',
                        '{fecha_actual}',
                        {f"'{fecha_valido_desde}'" if fecha_valido_desde else 'NULL'},
                        '{fecha_valido_hasta}'
                    )
                    """

                    self.session.sql(insert_query).collect()
                    results['inserted'] += 1
                    results['inserted_details'].append({
                        'RECHAZOID': rechazoid,
                        'PAISID': paisid,
                        'COD_PROD': cod_prod,
                        'GRPID': grpid,
                        'PROPSTID': propstid
                    })

                except Exception as e:
                    results['failed'] += 1
                    error_msg = f"RECHAZOID {rechazoid}: {str(e)}"
                    results['errors'].append(error_msg)

        except Exception as e:
            results['errors'].append(f"Error general: {str(e)}")

        return results

    def insert_homologaciones_sucursales_from_rechazos(self, rechazos_ids: List[int]) -> Dict[str, Any]:
        results = {
            'total': 0,
            'inserted': 0,
            'duplicated': 0,
            'failed': 0,
            'errors': [],
            'duplicates': [],
            'inserted_details': []
        }

        if not rechazos_ids:
            return results

        try:
            ids_str = ','.join(map(str, rechazos_ids))

            query_rechazos = f"""
            SELECT
                r.RECHAZOID,
                r.PAISID,
                r.VALOR_RECHAZADO as NUM_SUCURSAL,
                r.VALOR_HOMOLOGACION as SUCID,
                r.SEMANAS
            FROM {self.database}.{self.schema}.RECHAZOS_SEGUIMIENTO r
            WHERE r.RECHAZOID IN ({ids_str})
                AND r.RESPONSABLE_DE_CASO = 'Gobierno de Datos'
                AND r.MODULO = 'Sellout'
                AND r.CAMPO_RECHAZADO = 'SUCID'
                AND r.MOTIVO_RECHAZO = 'Sucursal no encontrada en tabla de homologación'
                AND r.CASO = 'Homologacion Sucursal'
                AND r.VALOR_HOMOLOGACION IS NOT NULL
            """

            rechazos_df = self.session.sql(query_rechazos).to_pandas()
            results['total'] = len(rechazos_df)

            if results['total'] == 0:
                return results

            for idx, row in rechazos_df.iterrows():
                rechazoid = row['RECHAZOID']
                paisid = row['PAISID']
                num_sucursal = str(row['NUM_SUCURSAL']).replace("'", "''") if pd.notna(row['NUM_SUCURSAL']) else ''
                sucid = str(row['SUCID']).replace("'", "''") if pd.notna(row['SUCID']) else ''
                semanas = row['SEMANAS']

                try:
                    suc_query = f"""
                    SELECT GRPID, CADID, SUCNOMBRE, DIRCALLE
                    FROM {self.database}.{self.schema}.VW_ESTRUCTURASUCURSALES
                    WHERE SUCID = '{sucid}'
                    LIMIT 1
                    """

                    suc_df = self.session.sql(suc_query).to_pandas()

                    if len(suc_df) == 0:
                        results['failed'] += 1
                        results['errors'].append(f"RECHAZOID {rechazoid}: No se encontró información para SUCID='{sucid}'")
                        continue

                    grpid = suc_df.iloc[0]['GRPID']
                    cadid = suc_df.iloc[0]['CADID']
                    descripcion = str(suc_df.iloc[0]['SUCNOMBRE']).replace("'", "''") if pd.notna(suc_df.iloc[0]['SUCNOMBRE']) else ''
                    direccion = str(suc_df.iloc[0]['DIRCALLE']).replace("'", "''") if pd.notna(suc_df.iloc[0]['DIRCALLE']) else ''

                    # Verificar duplicados
                    check_query = f"""
                    SELECT COUNT(*)
                    FROM {self.database}.{self.schema}.SUC_SO_HOMOLOGACION
                    WHERE PAISID = {paisid}
                        AND NUM_SUCURSAL = '{num_sucursal}'
                        AND GRPID = {grpid}
                    """

                    count_df = self.session.sql(check_query).to_pandas()
                    count = count_df.iloc[0, 0]

                    if count > 0:
                        results['duplicated'] += 1
                        results['duplicates'].append({
                            'RECHAZOID': rechazoid,
                            'PAISID': paisid,
                            'NUM_SUCURSAL': num_sucursal,
                            'GRPID': grpid,
                            'SUCID': sucid
                        })
                        continue

                    # Obtener FECHA_VALIDO_DESDE
                    fecha_valido_desde = None
                    if pd.notna(semanas):
                        try:
                            semanio = int(semanas) // 100
                            semnumero = int(semanas) % 100

                            semanas_query = f"""
                            SELECT SEMINICIO
                            FROM {self.database}.{self.schema}.CATSEMANAS
                            WHERE SEMANIO = {semanio}
                                AND SEMNUMERO = {semnumero}
                            """

                            semanas_df = self.session.sql(semanas_query).to_pandas()
                            if len(semanas_df) > 0:
                                fecha_valido_desde = semanas_df.iloc[0, 0]
                                if isinstance(fecha_valido_desde, datetime):
                                    fecha_valido_desde = fecha_valido_desde.strftime('%Y-%m-%d %H:%M:%S')
                        except Exception as e:
                            pass

                    fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    fecha_valido_hasta = '2999-12-31 00:00:00'

                    insert_query = f"""
                    INSERT INTO {self.database}.{self.schema}.SUC_SO_HOMOLOGACION (
                        PAISID,
                        GRPID,
                        CADID,
                        NUM_SUCURSAL,
                        DESCRIPCION,
                        DIRECCION,
                        SUCID,
                        ACTIVO,
                        CREATE_AT,
                        UPDATE_AT,
                        FECHA_VALIDO_DESDE,
                        FECHA_VALIDO_HASTA
                    ) VALUES (
                        {paisid},
                        {grpid},
                        {cadid},
                        '{num_sucursal}',
                        {f"'{descripcion}'" if descripcion else 'NULL'},
                        {f"'{direccion}'" if direccion else 'NULL'},
                        '{sucid}',
                        TRUE,
                        '{fecha_actual}',
                        '{fecha_actual}',
                        {f"'{fecha_valido_desde}'" if fecha_valido_desde else 'NULL'},
                        '{fecha_valido_hasta}'
                    )
                    """

                    self.session.sql(insert_query).collect()
                    results['inserted'] += 1
                    results['inserted_details'].append({
                        'RECHAZOID': rechazoid,
                        'PAISID': paisid,
                        'NUM_SUCURSAL': num_sucursal,
                        'GRPID': grpid,
                        'SUCID': sucid
                    })

                except Exception as e:
                    results['failed'] += 1
                    error_msg = f"RECHAZOID {rechazoid}: {str(e)}"
                    results['errors'].append(error_msg)

        except Exception as e:
            results['errors'].append(f"Error general: {str(e)}")

        return results

    def verify_table_exists(self) -> bool:
        try:
            query = f"""
            SELECT COUNT(*)
            FROM {self.database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{self.schema}'
            AND TABLE_NAME = 'RECHAZOS_SEGUIMIENTO'
            """

            result_df = self.session.sql(query).to_pandas()
            count = result_df.iloc[0, 0]

            return count > 0

        except Exception as e:
            return False

    def close(self):
        pass
