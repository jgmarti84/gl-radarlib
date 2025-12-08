# 8. Ejemplos Avanzados

## Ejemplo 1: Sistema de Alerta por Reflectividad Alta

Este ejemplo muestra cómo implementar un sistema de alertas basado en umbrales de reflectividad.

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sistema de Alerta por Reflectividad Alta.

Monitorea datos de radar en tiempo real y genera alertas cuando la
reflectividad supera umbrales definidos.
"""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Callable
import numpy as np

from radarlib.daemons import DaemonManager, DaemonManagerConfig
from radarlib.io.bufr.bufr_to_pyart import bufr_fields_to_pyart_radar
from radarlib.io.pyart.colmax import generate_colmax
from radarlib import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AlertaReflectividad:
    """Sistema de alertas por reflectividad."""

    def __init__(
        self,
        umbral_moderado: float = 35.0,
        umbral_fuerte: float = 45.0,
        umbral_severo: float = 55.0,
        callback_alerta: Optional[Callable] = None
    ):
        """
        Inicializar sistema de alertas.

        Args:
            umbral_moderado: dBZ para alerta moderada
            umbral_fuerte: dBZ para alerta fuerte
            umbral_severo: dBZ para alerta severa
            callback_alerta: Función a llamar cuando se genera alerta
        """
        self.umbrales = {
            'MODERADO': umbral_moderado,
            'FUERTE': umbral_fuerte,
            'SEVERO': umbral_severo
        }
        self.callback_alerta = callback_alerta
        self.historial_alertas: List[Dict] = []

    def analizar_radar(self, radar, nombre_radar: str) -> List[Dict]:
        """
        Analiza datos de radar y genera alertas.

        Args:
            radar: Objeto PyART Radar
            nombre_radar: Identificador del radar

        Returns:
            Lista de alertas generadas
        """
        alertas = []

        # Verificar campos disponibles
        if 'DBZH' not in radar.fields and 'COLMAX' not in radar.fields:
            logger.warning("No hay campos de reflectividad disponibles")
            return alertas

        # Usar COLMAX si está disponible, sino DBZH
        campo = 'COLMAX' if 'COLMAX' in radar.fields else 'DBZH'
        datos = radar.fields[campo]['data']

        # Calcular máximo (ignorando valores enmascarados)
        if hasattr(datos, 'compressed'):
            datos_validos = datos.compressed()
        else:
            datos_validos = datos[~np.isnan(datos)]

        if len(datos_validos) == 0:
            return alertas

        max_dbz = float(np.nanmax(datos_validos))

        # Determinar nivel de alerta
        nivel = None
        for nombre, umbral in sorted(self.umbrales.items(),
                                     key=lambda x: x[1],
                                     reverse=True):
            if max_dbz >= umbral:
                nivel = nombre
                break

        if nivel:
            alerta = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'radar': nombre_radar,
                'nivel': nivel,
                'max_dbz': max_dbz,
                'campo': campo,
                'ubicacion_radar': {
                    'lat': float(radar.latitude['data'][0]),
                    'lon': float(radar.longitude['data'][0])
                }
            }

            alertas.append(alerta)
            self.historial_alertas.append(alerta)

            # Ejecutar callback si está definido
            if self.callback_alerta:
                self.callback_alerta(alerta)

            logger.warning(
                f"⚠️ ALERTA {nivel} - {nombre_radar}: "
                f"Reflectividad máxima {max_dbz:.1f} dBZ"
            )

        return alertas

    def resumen_alertas(self) -> Dict:
        """Genera resumen de alertas del historial."""
        if not self.historial_alertas:
            return {'total': 0, 'por_nivel': {}, 'por_radar': {}}

        por_nivel = {}
        por_radar = {}

        for alerta in self.historial_alertas:
            nivel = alerta['nivel']
            radar = alerta['radar']

            por_nivel[nivel] = por_nivel.get(nivel, 0) + 1
            por_radar[radar] = por_radar.get(radar, 0) + 1

        return {
            'total': len(self.historial_alertas),
            'por_nivel': por_nivel,
            'por_radar': por_radar,
            'ultima_alerta': self.historial_alertas[-1] if self.historial_alertas else None
        }


def callback_enviar_email(alerta: Dict):
    """Callback de ejemplo para enviar alertas por email."""
    logger.info(f"📧 Enviando email de alerta: {alerta['nivel']} en {alerta['radar']}")
    # Aquí iría la lógica de envío de email


async def main():
    """Ejemplo de uso del sistema de alertas."""

    # Crear sistema de alertas
    sistema_alertas = AlertaReflectividad(
        umbral_moderado=35.0,
        umbral_fuerte=45.0,
        umbral_severo=55.0,
        callback_alerta=callback_enviar_email
    )

    # Simulación: crear radar de ejemplo con datos aleatorios
    import pyart

    radar = pyart.testing.make_empty_ppi_radar(500, 360, 1)

    # Agregar campo de reflectividad con algunos valores altos
    reflectividad = np.random.uniform(-10, 60, (360, 500))
    reflectividad[100:150, 200:250] = 50  # Área de alta reflectividad

    radar.add_field('DBZH', {'data': np.ma.array(reflectividad)})

    # Analizar radar
    alertas = sistema_alertas.analizar_radar(radar, "RMA1")

    # Mostrar resumen
    print("\n" + "="*50)
    print("RESUMEN DE ALERTAS")
    print("="*50)
    resumen = sistema_alertas.resumen_alertas()
    print(f"Total de alertas: {resumen['total']}")
    print(f"Por nivel: {resumen['por_nivel']}")
    print(f"Por radar: {resumen['por_radar']}")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Ejemplo 2: Análisis de Precipitación con Múltiples Radares

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Análisis de Precipitación Multi-Radar.

Combina datos de múltiples radares para generar un mapa compuesto
de precipitación.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import numpy as np

from radarlib.io.bufr.bufr import bufr_to_dict
from radarlib.io.bufr.bufr_to_pyart import bufr_fields_to_pyart_radar
from radarlib.io.pyart.radar_png_plotter import (
    RadarPlotConfig, plot_and_save_ppi
)
from radarlib import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnalisisPrecipitacion:
    """Análisis de precipitación multi-radar."""

    # Relación Z-R: Z = a * R^b (Marshall-Palmer)
    COEF_A = 200.0
    COEF_B = 1.6

    def __init__(self, output_dir: Path):
        """
        Inicializar analizador.

        Args:
            output_dir: Directorio de salida para productos
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def dbz_a_precipitacion(self, dbz: np.ndarray) -> np.ndarray:
        """
        Convierte reflectividad (dBZ) a tasa de precipitación (mm/h).

        Usa la relación Z-R de Marshall-Palmer:
        Z = 200 * R^1.6

        Args:
            dbz: Array de reflectividad en dBZ

        Returns:
            Array de tasa de precipitación en mm/h
        """
        # Convertir dBZ a Z (mm^6/m^3)
        z = 10.0 ** (dbz / 10.0)

        # Convertir Z a R (mm/h)
        r = (z / self.COEF_A) ** (1.0 / self.COEF_B)

        return r

    def procesar_radar(
        self,
        bufr_files: List[str],
        nombre_radar: str
    ) -> Optional[Dict]:
        """
        Procesa archivos BUFR de un radar.

        Args:
            bufr_files: Lista de archivos BUFR
            nombre_radar: Nombre del radar

        Returns:
            Dict con datos procesados o None si falla
        """
        # Decodificar archivos
        volumenes = []
        for archivo in bufr_files:
            vol = bufr_to_dict(archivo)
            if vol:
                volumenes.append(vol)

        if not volumenes:
            logger.error(f"No se pudieron decodificar archivos de {nombre_radar}")
            return None

        # Crear objeto Radar
        try:
            radar = bufr_fields_to_pyart_radar(volumenes)
        except Exception as e:
            logger.error(f"Error creando radar {nombre_radar}: {e}")
            return None

        # Verificar campo de reflectividad
        if 'DBZH' not in radar.fields:
            logger.warning(f"No hay DBZH en {nombre_radar}")
            return None

        # Obtener datos del barrido más bajo
        dbzh = radar.fields['DBZH']['data'][0:360, :]  # Primer barrido

        # Calcular precipitación
        precip = self.dbz_a_precipitacion(dbzh)

        # Estadísticas
        precip_valido = precip[~np.isnan(precip) & (precip > 0)]

        resultado = {
            'radar': nombre_radar,
            'lat': float(radar.latitude['data'][0]),
            'lon': float(radar.longitude['data'][0]),
            'reflectividad_max': float(np.nanmax(dbzh)),
            'reflectividad_media': float(np.nanmean(dbzh[dbzh > 0])) if np.any(dbzh > 0) else 0,
            'precip_max': float(np.nanmax(precip)) if len(precip_valido) > 0 else 0,
            'precip_media': float(np.nanmean(precip_valido)) if len(precip_valido) > 0 else 0,
            'area_precipitacion': float(len(precip_valido)) / float(precip.size) * 100,  # %
            'radar_object': radar,
            'datos_precip': precip
        }

        logger.info(
            f"{nombre_radar}: Max={resultado['reflectividad_max']:.1f} dBZ, "
            f"Precip Max={resultado['precip_max']:.1f} mm/h, "
            f"Área={resultado['area_precipitacion']:.1f}%"
        )

        return resultado

    def generar_reporte(
        self,
        resultados: List[Dict],
        nombre_reporte: str = "reporte_precipitacion"
    ) -> Path:
        """
        Genera reporte de análisis de precipitación.

        Args:
            resultados: Lista de resultados por radar
            nombre_reporte: Nombre base del archivo de reporte

        Returns:
            Path al archivo de reporte generado
        """
        from datetime import datetime

        reporte_path = self.output_dir / f"{nombre_reporte}.md"

        with open(reporte_path, 'w', encoding='utf-8') as f:
            f.write("# Reporte de Análisis de Precipitación\n\n")
            f.write(f"**Fecha de generación:** {datetime.now().isoformat()}\n\n")

            f.write("## Resumen por Radar\n\n")
            f.write("| Radar | Lat | Lon | dBZ Max | dBZ Med | Precip Max (mm/h) | Área (%) |\n")
            f.write("|-------|-----|-----|---------|---------|-------------------|----------|\n")

            for r in resultados:
                f.write(
                    f"| {r['radar']} | {r['lat']:.4f} | {r['lon']:.4f} | "
                    f"{r['reflectividad_max']:.1f} | {r['reflectividad_media']:.1f} | "
                    f"{r['precip_max']:.1f} | {r['area_precipitacion']:.1f} |\n"
                )

            # Estadísticas generales
            f.write("\n## Estadísticas Generales\n\n")

            max_global = max(r['reflectividad_max'] for r in resultados)
            precip_max_global = max(r['precip_max'] for r in resultados)

            f.write(f"- **Reflectividad máxima global:** {max_global:.1f} dBZ\n")
            f.write(f"- **Precipitación máxima global:** {precip_max_global:.1f} mm/h\n")
            f.write(f"- **Radares analizados:** {len(resultados)}\n")

        logger.info(f"Reporte generado: {reporte_path}")
        return reporte_path


def main():
    """Ejemplo de análisis multi-radar."""

    analizador = AnalisisPrecipitacion(
        output_dir=Path("./analisis_precipitacion")
    )

    # Definir archivos por radar (ejemplo)
    radares = {
        "RMA1": [
            "datos/RMA1_0315_01_DBZH_20250101T120000Z.BUFR",
        ],
        "RMA3": [
            "datos/RMA3_0315_01_DBZH_20250101T120000Z.BUFR",
        ],
    }

    # Procesar cada radar
    resultados = []
    for nombre, archivos in radares.items():
        resultado = analizador.procesar_radar(archivos, nombre)
        if resultado:
            resultados.append(resultado)

    # Generar reporte
    if resultados:
        analizador.generar_reporte(resultados)


if __name__ == "__main__":
    main()
```

---

## Ejemplo 3: Integración con Base de Datos PostgreSQL

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Integración con PostgreSQL para almacenamiento de metadatos de radar.

Almacena información de volúmenes procesados en una base de datos
PostgreSQL para consultas y análisis histórico.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import json

# Nota: Requiere psycopg2 instalado
# pip install psycopg2-binary

from radarlib.io.bufr.bufr import bufr_to_dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RadarPostgresStore:
    """Almacén de datos de radar en PostgreSQL."""

    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS radar_volumes (
        id SERIAL PRIMARY KEY,
        radar_name VARCHAR(10) NOT NULL,
        strategy VARCHAR(10) NOT NULL,
        vol_nr VARCHAR(5) NOT NULL,
        field_type VARCHAR(20) NOT NULL,
        observation_time TIMESTAMP WITH TIME ZONE NOT NULL,
        lat DOUBLE PRECISION,
        lon DOUBLE PRECISION,
        altitude DOUBLE PRECISION,
        nsweeps INTEGER,
        ngates INTEGER,
        nrays INTEGER,
        max_value DOUBLE PRECISION,
        mean_value DOUBLE PRECISION,
        min_value DOUBLE PRECISION,
        metadata JSONB,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(radar_name, strategy, vol_nr, field_type, observation_time)
    );

    CREATE INDEX IF NOT EXISTS idx_radar_volumes_radar_time
        ON radar_volumes(radar_name, observation_time);
    CREATE INDEX IF NOT EXISTS idx_radar_volumes_time
        ON radar_volumes(observation_time);
    """

    INSERT_SQL = """
    INSERT INTO radar_volumes (
        radar_name, strategy, vol_nr, field_type, observation_time,
        lat, lon, altitude, nsweeps, ngates, nrays,
        max_value, mean_value, min_value, metadata
    ) VALUES (
        %(radar_name)s, %(strategy)s, %(vol_nr)s, %(field_type)s,
        %(observation_time)s, %(lat)s, %(lon)s, %(altitude)s,
        %(nsweeps)s, %(ngates)s, %(nrays)s,
        %(max_value)s, %(mean_value)s, %(min_value)s,
        %(metadata)s
    )
    ON CONFLICT (radar_name, strategy, vol_nr, field_type, observation_time)
    DO UPDATE SET
        max_value = EXCLUDED.max_value,
        mean_value = EXCLUDED.mean_value,
        min_value = EXCLUDED.min_value,
        metadata = EXCLUDED.metadata;
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "radar_db",
        user: str = "radar_user",
        password: str = "password"
    ):
        """
        Inicializar conexión a PostgreSQL.

        Args:
            host: Host del servidor PostgreSQL
            port: Puerto del servidor
            database: Nombre de la base de datos
            user: Usuario de la base de datos
            password: Contraseña
        """
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 es requerido. Instálelo con: "
                "pip install psycopg2-binary"
            )

        self.conn_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }

        self._conn = None
        self._init_database()

    def _get_connection(self):
        """Obtener conexión activa."""
        import psycopg2

        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**self.conn_params)
        return self._conn

    def _init_database(self):
        """Inicializar esquema de base de datos."""
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(self.CREATE_TABLE_SQL)
        conn.commit()
        logger.info("Esquema de base de datos inicializado")

    def store_volume(self, bufr_result: Dict) -> bool:
        """
        Almacenar información de volumen en la base de datos.

        Args:
            bufr_result: Resultado de bufr_to_dict

        Returns:
            True si se almacenó correctamente
        """
        import numpy as np

        if bufr_result is None:
            return False

        info = bufr_result['info']
        data = bufr_result['data']

        # Calcular estadísticas de datos
        data_valid = data[~np.isnan(data)]

        # Construir timestamp de observación
        obs_time = datetime(
            info['ano_vol'], info['mes_vol'], info['dia_vol'],
            info['hora_vol'], info['min_vol'], 0,
            tzinfo=timezone.utc
        )

        record = {
            'radar_name': info['nombre_radar'],
            'strategy': info['estrategia']['nombre'],
            'vol_nr': info['estrategia']['volume_number'],
            'field_type': info['tipo_producto'],
            'observation_time': obs_time,
            'lat': info['lat'],
            'lon': info['lon'],
            'altitude': info['altura'],
            'nsweeps': info['nsweeps'],
            'ngates': data.shape[1] if len(data.shape) > 1 else 0,
            'nrays': data.shape[0],
            'max_value': float(np.nanmax(data_valid)) if len(data_valid) > 0 else None,
            'mean_value': float(np.nanmean(data_valid)) if len(data_valid) > 0 else None,
            'min_value': float(np.nanmin(data_valid)) if len(data_valid) > 0 else None,
            'metadata': json.dumps(info.get('metadata', {}))
        }

        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute(self.INSERT_SQL, record)
            conn.commit()
            logger.info(
                f"Volumen almacenado: {record['radar_name']} "
                f"{record['field_type']} {obs_time.isoformat()}"
            )
            return True
        except Exception as e:
            logger.error(f"Error almacenando volumen: {e}")
            conn.rollback()
            return False

    def query_by_radar_and_time(
        self,
        radar_name: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict]:
        """
        Consultar volúmenes por radar y rango de tiempo.

        Args:
            radar_name: Nombre del radar
            start_time: Inicio del rango
            end_time: Fin del rango

        Returns:
            Lista de registros
        """
        query = """
        SELECT * FROM radar_volumes
        WHERE radar_name = %s
          AND observation_time >= %s
          AND observation_time <= %s
        ORDER BY observation_time;
        """

        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(query, (radar_name, start_time, end_time))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_statistics(self, radar_name: str, days: int = 7) -> Dict:
        """
        Obtener estadísticas de los últimos N días.

        Args:
            radar_name: Nombre del radar
            days: Número de días a analizar

        Returns:
            Dict con estadísticas
        """
        query = """
        SELECT
            field_type,
            COUNT(*) as count,
            AVG(max_value) as avg_max,
            MAX(max_value) as overall_max,
            AVG(mean_value) as avg_mean
        FROM radar_volumes
        WHERE radar_name = %s
          AND observation_time >= NOW() - INTERVAL '%s days'
        GROUP BY field_type;
        """

        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(query, (radar_name, days))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def close(self):
        """Cerrar conexión."""
        if self._conn:
            self._conn.close()
            self._conn = None


def main():
    """Ejemplo de uso del almacén PostgreSQL."""

    # Crear almacén (ajustar credenciales según su configuración)
    store = RadarPostgresStore(
        host="localhost",
        database="radar_db",
        user="radar_user",
        password="password"
    )

    try:
        # Procesar y almacenar archivos BUFR
        bufr_files = [
            "datos/RMA1_0315_01_DBZH_20250101T120000Z.BUFR",
            "datos/RMA1_0315_01_VRAD_20250101T120000Z.BUFR",
        ]

        for bufr_file in bufr_files:
            resultado = bufr_to_dict(bufr_file)
            if resultado:
                store.store_volume(resultado)

        # Consultar estadísticas
        stats = store.get_statistics("RMA1", days=7)
        print("\nEstadísticas de los últimos 7 días:")
        for s in stats:
            print(f"  {s['field_type']}: {s['count']} volúmenes, "
                  f"max promedio = {s['avg_max']:.1f}")

    finally:
        store.close()


if __name__ == "__main__":
    main()
```

---

## Ejemplo 4: Exportación a Formato GRIB2

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Exportación de datos de radar a formato GRIB2.

Convierte datos de PyART Radar a formato GRIB2 para intercambio
con sistemas de modelado numérico.
"""

import logging
from pathlib import Path
from typing import Optional
import numpy as np

# Nota: Requiere pygrib instalado
# pip install pygrib

from radarlib.io.bufr.bufr import bufr_to_dict
from radarlib.io.bufr.bufr_to_pyart import bufr_fields_to_pyart_radar

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def radar_to_grib2(
    radar,
    output_file: Path,
    field: str = "DBZH",
    sweep: int = 0
) -> Path:
    """
    Exporta datos de radar a formato GRIB2.

    Args:
        radar: Objeto PyART Radar
        output_file: Archivo de salida
        field: Campo a exportar
        sweep: Índice del barrido

    Returns:
        Path al archivo GRIB2 generado

    Note:
        Esta es una implementación de ejemplo. Para producción,
        se recomienda usar las herramientas oficiales de la WMO.
    """
    try:
        import eccodes
    except ImportError:
        raise ImportError(
            "eccodes es requerido. Instálelo con: "
            "pip install eccodes"
        )

    if field not in radar.fields:
        raise ValueError(f"Campo {field} no encontrado en radar")

    # Obtener datos del barrido
    sweep_start = radar.sweep_start_ray_index['data'][sweep]
    sweep_end = radar.sweep_end_ray_index['data'][sweep]

    data = radar.fields[field]['data'][sweep_start:sweep_end+1, :]

    # Información geográfica
    lat = float(radar.latitude['data'][0])
    lon = float(radar.longitude['data'][0])

    # Crear mensaje GRIB2
    logger.info(f"Creando GRIB2: {output_file}")

    # Aquí iría la lógica de creación de GRIB2 usando eccodes
    # Esta es una implementación simplificada de ejemplo

    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # ... código de eccodes para escribir GRIB2 ...

    logger.info(f"GRIB2 generado: {output_file}")
    return output_file
```

---

*Esta documentación es parte del proyecto radarlib desarrollado por el Grupo Radar Córdoba (GRC).*
