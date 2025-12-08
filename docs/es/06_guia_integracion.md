# 6. Guía de Integración

## Integración desde Otro Proyecto

Esta guía muestra cómo integrar radarlib en su proyecto Python existente para procesar y visualizar datos de radar.

### Ejemplo Completo: Sistema de Monitoreo de Radar

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sistema de Monitoreo de Radar usando radarlib.

Este ejemplo muestra cómo integrar radarlib en un proyecto de monitoreo
meteorológico para procesar datos de radar en tiempo real.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

# Configurar variables de entorno ANTES de importar radarlib
os.environ['FTP_HOST'] = 'ftp.miservidor.com'
os.environ['FTP_USER'] = 'mi_usuario'
os.environ['FTP_PASS'] = 'mi_contraseña'
os.environ['ROOT_RADAR_FILES_PATH'] = '/datos/radares'
os.environ['ROOT_RADAR_PRODUCTS_PATH'] = '/productos/radar'
os.environ['PNG_DPI'] = '150'

# Ahora importar radarlib
from radarlib import config
from radarlib.daemons import DaemonManager, DaemonManagerConfig
from radarlib.io.bufr.bufr import bufr_to_dict
from radarlib.io.bufr.bufr_to_pyart import bufr_fields_to_pyart_radar
from radarlib.io.pyart.colmax import generate_colmax
from radarlib.io.pyart.radar_png_plotter import (
    RadarPlotConfig,
    plot_multiple_fields,
    export_fields_to_geotiff
)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RadarMonitoringSystem:
    """Sistema de monitoreo de radar integrado con radarlib."""

    def __init__(
        self,
        radar_codes: List[str],
        base_output_dir: Path,
        ftp_config: Dict[str, str],
        volume_types: Dict
    ):
        """
        Inicializar el sistema de monitoreo.

        Args:
            radar_codes: Lista de códigos de radar a monitorear (ej: ["RMA1", "RMA3"])
            base_output_dir: Directorio base para salida
            ftp_config: Configuración FTP {'host', 'user', 'password', 'base_path'}
            volume_types: Configuración de tipos de volumen
        """
        self.radar_codes = radar_codes
        self.base_output_dir = Path(base_output_dir)
        self.ftp_config = ftp_config
        self.volume_types = volume_types
        self.managers: Dict[str, DaemonManager] = {}

        # Crear directorios
        self.base_output_dir.mkdir(parents=True, exist_ok=True)

    def create_daemon_config(self, radar_code: str) -> DaemonManagerConfig:
        """Crear configuración de daemon para un radar específico."""
        radar_path = self.base_output_dir / radar_code

        return DaemonManagerConfig(
            radar_name=radar_code,
            base_path=radar_path,
            ftp_host=self.ftp_config['host'],
            ftp_user=self.ftp_config['user'],
            ftp_password=self.ftp_config['password'],
            ftp_base_path=self.ftp_config['base_path'],
            volume_types=self.volume_types,
            start_date=datetime.now(timezone.utc),
            enable_download_daemon=True,
            enable_processing_daemon=True,
            enable_product_daemon=True,
            enable_cleanup_daemon=True,
            download_poll_interval=60,
            processing_poll_interval=30,
            product_poll_interval=30,
            cleanup_poll_interval=3600,
            bufr_retention_days=3,
            netcdf_retention_days=7,
            product_type="image",
            add_colmax=True,
        )

    async def start_monitoring(self):
        """Iniciar monitoreo de todos los radares."""
        tasks = []

        for radar_code in self.radar_codes:
            logger.info(f"Iniciando monitoreo para {radar_code}")

            config = self.create_daemon_config(radar_code)
            manager = DaemonManager(config)
            self.managers[radar_code] = manager

            task = asyncio.create_task(manager.start())
            tasks.append(task)

        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Deteniendo todos los daemons...")
            for manager in self.managers.values():
                manager.stop()

    def get_status(self) -> Dict:
        """Obtener estado de todos los radares monitoreados."""
        status = {}
        for radar_code, manager in self.managers.items():
            status[radar_code] = manager.get_status()
        return status

    def stop_all(self):
        """Detener todos los daemons."""
        for manager in self.managers.values():
            manager.stop()


class ManualRadarProcessor:
    """Procesador manual de archivos de radar."""

    def __init__(self, output_dir: Path):
        """
        Inicializar el procesador.

        Args:
            output_dir: Directorio de salida para productos
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def process_bufr_files(
        self,
        bufr_files: List[str],
        generate_colmax_field: bool = True,
        fields_to_plot: Optional[List[str]] = None
    ) -> Dict:
        """
        Procesar una lista de archivos BUFR y generar productos.

        Args:
            bufr_files: Lista de rutas a archivos BUFR
            generate_colmax_field: Si generar campo COLMAX
            fields_to_plot: Campos a graficar (None = usar configuración)

        Returns:
            Dict con resultados del procesamiento
        """
        if fields_to_plot is None:
            fields_to_plot = config.FIELDS_TO_PLOT

        results = {
            'decoded_volumes': [],
            'radar_object': None,
            'png_files': {},
            'geotiff_files': {},
            'errors': []
        }

        # Paso 1: Decodificar archivos BUFR
        logger.info(f"Decodificando {len(bufr_files)} archivos BUFR...")
        for bufr_file in bufr_files:
            try:
                vol = bufr_to_dict(bufr_file)
                if vol is not None:
                    results['decoded_volumes'].append(vol)
                    logger.info(f"  ✓ {Path(bufr_file).name}")
                else:
                    results['errors'].append(f"Error decodificando: {bufr_file}")
                    logger.warning(f"  ✗ {Path(bufr_file).name}")
            except Exception as e:
                results['errors'].append(f"Excepción en {bufr_file}: {e}")
                logger.error(f"  ✗ {Path(bufr_file).name}: {e}")

        if not results['decoded_volumes']:
            logger.error("No se pudieron decodificar archivos")
            return results

        # Paso 2: Crear objeto Radar PyART
        logger.info("Creando objeto Radar PyART...")
        try:
            radar = bufr_fields_to_pyart_radar(results['decoded_volumes'])
            results['radar_object'] = radar
            logger.info(f"  Campos disponibles: {list(radar.fields.keys())}")
        except Exception as e:
            results['errors'].append(f"Error creando Radar: {e}")
            logger.error(f"Error creando objeto Radar: {e}")
            return results

        # Paso 3: Generar COLMAX (opcional)
        if generate_colmax_field:
            logger.info("Generando campo COLMAX...")
            try:
                radar = generate_colmax(
                    radar=radar,
                    elev_limit1=config.COLMAX_ELEV_LIMIT1,
                    RHOHV_filter=config.COLMAX_RHOHV_FILTER,
                    RHOHV_umbral=config.COLMAX_RHOHV_UMBRAL,
                    WRAD_filter=config.COLMAX_WRAD_FILTER,
                    WRAD_umbral=config.COLMAX_WRAD_UMBRAL,
                    save_changes=False
                )
                results['radar_object'] = radar
                logger.info("  ✓ COLMAX generado")
            except Exception as e:
                logger.warning(f"  No se pudo generar COLMAX: {e}")

        # Paso 4: Generar gráficos PNG
        logger.info("Generando gráficos PNG...")
        png_dir = self.output_dir / "png"
        png_dir.mkdir(exist_ok=True)

        plot_config = RadarPlotConfig(
            figsize=(15, 15),
            dpi=config.PNG_DPI,
            transparent=True
        )

        # Filtrar campos disponibles
        available_fields = [f for f in fields_to_plot if f in radar.fields]

        try:
            results['png_files'] = plot_multiple_fields(
                radar,
                fields=available_fields,
                output_base_path=str(png_dir),
                sweep=0,
                config=plot_config
            )
            logger.info(f"  ✓ {len(results['png_files'])} imágenes generadas")
        except Exception as e:
            results['errors'].append(f"Error generando PNG: {e}")
            logger.error(f"Error generando PNG: {e}")

        # Paso 5: Exportar GeoTIFF
        logger.info("Exportando GeoTIFF...")
        geotiff_dir = self.output_dir / "geotiff"
        geotiff_dir.mkdir(exist_ok=True)

        try:
            results['geotiff_files'] = export_fields_to_geotiff(
                radar,
                fields=available_fields,
                output_base_path=str(geotiff_dir),
                sweep=0,
                crs="EPSG:4326"
            )
            logger.info(f"  ✓ {len(results['geotiff_files'])} GeoTIFF exportados")
        except Exception as e:
            results['errors'].append(f"Error exportando GeoTIFF: {e}")
            logger.error(f"Error exportando GeoTIFF: {e}")

        return results

    def save_radar_netcdf(self, radar, filename: str) -> Path:
        """Guardar objeto Radar a NetCDF."""
        from radarlib.io.bufr.bufr_to_pyart import save_radar_to_cfradial

        netcdf_dir = self.output_dir / "netcdf"
        netcdf_dir.mkdir(exist_ok=True)
        output_file = netcdf_dir / filename

        return save_radar_to_cfradial(radar, output_file)


def ejemplo_monitoreo_automatico():
    """Ejemplo: Sistema de monitoreo automático."""
    print("=" * 60)
    print("Ejemplo: Sistema de Monitoreo Automático de Radar")
    print("=" * 60)

    # Configuración
    volume_types = {
        "0315": {
            "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
            "02": ["VRAD", "WRAD"]
        }
    }

    ftp_config = {
        'host': config.FTP_HOST,
        'user': config.FTP_USER,
        'password': config.FTP_PASS,
        'base_path': '/L2'
    }

    # Crear sistema de monitoreo
    sistema = RadarMonitoringSystem(
        radar_codes=["RMA1", "RMA3"],
        base_output_dir=Path("./monitoreo_radar"),
        ftp_config=ftp_config,
        volume_types=volume_types
    )

    print("\nIniciando monitoreo...")
    print("Presione Ctrl+C para detener\n")

    try:
        asyncio.run(sistema.start_monitoring())
    except KeyboardInterrupt:
        print("\nDeteniendo sistema...")
        sistema.stop_all()

    # Mostrar estado final
    status = sistema.get_status()
    for radar, radar_status in status.items():
        print(f"\n{radar}:")
        print(f"  Download: {radar_status['download_daemon']['running']}")
        print(f"  Processing: {radar_status['processing_daemon']['running']}")


def ejemplo_procesamiento_manual():
    """Ejemplo: Procesamiento manual de archivos BUFR."""
    print("=" * 60)
    print("Ejemplo: Procesamiento Manual de Archivos BUFR")
    print("=" * 60)

    # Lista de archivos BUFR a procesar
    bufr_files = [
        "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",
        "RMA1_0315_01_DBZV_20250101T120000Z.BUFR",
        "RMA1_0315_01_ZDR_20250101T120000Z.BUFR",
        "RMA1_0315_01_RHOHV_20250101T120000Z.BUFR",
        "RMA1_0315_02_VRAD_20250101T120000Z.BUFR",
        "RMA1_0315_02_WRAD_20250101T120000Z.BUFR",
    ]

    # Crear procesador
    procesador = ManualRadarProcessor(
        output_dir=Path("./procesamiento_manual")
    )

    # Procesar archivos
    resultados = procesador.process_bufr_files(
        bufr_files=bufr_files,
        generate_colmax_field=True,
        fields_to_plot=["DBZH", "VRAD", "ZDR", "RHOHV", "COLMAX"]
    )

    # Mostrar resultados
    print("\nResultados:")
    print(f"  Volúmenes decodificados: {len(resultados['decoded_volumes'])}")
    print(f"  Imágenes PNG: {len(resultados['png_files'])}")
    print(f"  Archivos GeoTIFF: {len(resultados['geotiff_files'])}")

    if resultados['errors']:
        print(f"\nErrores ({len(resultados['errors'])}):")
        for error in resultados['errors']:
            print(f"  - {error}")

    # Guardar NetCDF
    if resultados['radar_object'] is not None:
        nc_path = procesador.save_radar_netcdf(
            resultados['radar_object'],
            "RMA1_0315_01_20250101T120000Z.nc"
        )
        print(f"\nNetCDF guardado: {nc_path}")


def ejemplo_consulta_simple():
    """Ejemplo: Consulta simple de configuración."""
    print("=" * 60)
    print("Ejemplo: Consulta de Configuración")
    print("=" * 60)

    print(f"\nConfiguración actual de radarlib:")
    print(f"  FTP Host: {config.FTP_HOST}")
    print(f"  FTP User: {config.FTP_USER}")
    print(f"  Directorio de datos: {config.ROOT_RADAR_FILES_PATH}")
    print(f"  Directorio de productos: {config.ROOT_RADAR_PRODUCTS_PATH}")
    print(f"  PNG DPI: {config.PNG_DPI}")
    print(f"  Campos a graficar: {config.FIELDS_TO_PLOT}")
    print(f"  Umbral COLMAX RHOHV: {config.COLMAX_RHOHV_UMBRAL}")


if __name__ == "__main__":
    # Ejecutar ejemplo de consulta simple
    ejemplo_consulta_simple()

    # Descomentar para ejecutar otros ejemplos:
    # ejemplo_procesamiento_manual()
    # ejemplo_monitoreo_automatico()
```

### Estructura de Proyecto Recomendada

```
mi_proyecto_radar/
├── src/
│   ├── __init__.py
│   ├── monitoring/
│   │   ├── __init__.py
│   │   ├── radar_system.py      # Sistema de monitoreo
│   │   └── alerts.py            # Sistema de alertas
│   ├── processing/
│   │   ├── __init__.py
│   │   ├── processor.py         # Procesador de datos
│   │   └── products.py          # Generador de productos
│   └── utils/
│       ├── __init__.py
│       └── config_loader.py     # Configuración personalizada
├── config/
│   ├── radarlib.json            # Configuración de radarlib
│   └── radars.yaml              # Configuración de radares
├── data/
│   ├── bufr/                    # Archivos BUFR descargados
│   ├── netcdf/                  # Archivos NetCDF
│   └── products/                # Productos generados
├── logs/
│   └── monitoring.log           # Logs del sistema
├── tests/
│   ├── __init__.py
│   ├── test_processing.py
│   └── test_products.py
├── requirements.txt
├── pyproject.toml
└── README.md
```

### Configuración Personalizada

**config/radarlib.json:**

```json
{
    "FTP_HOST": "ftp.smn.gob.ar",
    "FTP_USER": "mi_usuario",
    "FTP_PASS": "mi_contraseña",
    "ROOT_RADAR_FILES_PATH": "./data",
    "ROOT_RADAR_PRODUCTS_PATH": "./data/products",
    "PNG_DPI": 150,
    "FIELDS_TO_PLOT": ["DBZH", "VRAD", "ZDR", "COLMAX"],
    "COLMAX_RHOHV_UMBRAL": 0.85,
    "GRC_RHV_THRESHOLD": 0.6
}
```

**requirements.txt:**

```
radarlib>=0.1.0
numpy>=2.0.0
pandas>=2.0.0
arm-pyart>=2.1.0
matplotlib>=3.9.0
aiohttp>=3.9.0
pyyaml>=6.0.0
```

### Uso con Docker

**Dockerfile:**

```dockerfile
FROM python:3.11-slim

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    libnetcdf-dev \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*

# Crear directorio de trabajo
WORKDIR /app

# Copiar requirements e instalar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY . .

# Variables de entorno
ENV RADARLIB_CONFIG=/app/config/radarlib.json
ENV MPLBACKEND=Agg

# Comando por defecto
CMD ["python", "-m", "src.monitoring.radar_system"]
```

**docker-compose.yml:**

```yaml
version: '3.8'

services:
  radar-monitor:
    build: .
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./config:/app/config:ro
    environment:
      - FTP_HOST=${FTP_HOST}
      - FTP_USER=${FTP_USER}
      - FTP_PASS=${FTP_PASS}
      - RADARLIB_CONFIG=/app/config/radarlib.json
    restart: unless-stopped
```

---

*Continúe con el capítulo [Referencia de API](./07_referencia_api.md) para documentación detallada de funciones.*
