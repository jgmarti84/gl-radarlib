# radarlib

![Versión](https://img.shields.io/badge/versión-0.1.0-blue)
![Python](https://img.shields.io/badge/python-%3E%3D3.11-brightgreen)
![Licencia](https://img.shields.io/badge/licencia-MIT-green)

## Descripción General

**radarlib** es una biblioteca Python profesional para la obtención, procesamiento y
visualización de datos de radar meteorológico. Está desarrollada y mantenida por el
**Grupo Radar Córdoba (GRC)** y está diseñada para servir tanto a sistemas operativos
como a flujos de trabajo de investigación.

### Qué resuelve radarlib

Las redes de radar meteorológico producen continuamente grandes volúmenes de datos binarios
en formatos propietarios o especializados (BUFR, NetCDF, IRIS/SIGMET, etc.).
Convertir escaneos de radar sin procesar en productos accionables — mapas PNG
georeferenciados o rásters GeoTIFF optimizados para la nube adecuados para servicios
posteriores — requiere un pipeline confiable y automatizado. `radarlib` proporciona
exactamente ese pipeline:

```
Servidor FTP (BUFR)
      │
      ▼
DownloadDaemon          ← monitorea FTP remoto, checksums, reintentos
      │
      ▼  (BD SQLite de estado)
ProcessingDaemon        ← decodifica BUFR → volúmenes NetCDF vía PyART
      │
      ▼  (BD SQLite de estado)
ProductGenerationDaemon ← genera imágenes PNG / GeoTIFF optimizados para la nube
      │
      ▼  (BD SQLite de estado)
CleanupDaemon           ← aplica políticas de retención de datos
```

### Formatos y fuentes de datos soportados

| Categoría | Detalles |
|---|---|
| **Formato de entrada** | BUFR (Forma Universal Binaria para la Representación de datos meteorológicos) |
| **Formato intermedio** | NetCDF-4 / CF-Radial (vía arm-pyart) |
| **Formatos de salida** | Imágenes PNG, GeoTIFF optimizado para la nube (COG) |
| **Transporte de datos** | FTP asíncrono (vía `aioftp`) |
| **Red de radares** | Red SiNaRaMe de Argentina (códigos RMA*) y cualquier radar compatible con BUFR |

### Arquitectura de alto nivel

| Módulo | Función |
|---|---|
| `radarlib.daemons` | Trabajadores asincrónicos en segundo plano (descarga → procesamiento → producto → limpieza) |
| `radarlib.io.bufr` | Decodificación BUFR de bajo nivel usando bibliotecas compartidas Fortran/C |
| `radarlib.io.ftp` | Cliente FTP asíncrono con reintentos y verificación de checksum |
| `radarlib.io.pyart` | Integración con PyART: filtrado de campos, cálculo COLMAX, exportación PNG/GeoTIFF |
| `radarlib.radar_grid` | Motor de cuadrícula polar-a-cartesiana con geometría precomputada |
| `radarlib.state` | Seguimiento de estado SQLite y JSON para coordinación del pipeline |
| `radarlib.colormaps` | Mapas de color matplotlib personalizados para radar de polarización dual |
| `radarlib.config` | Configuración centralizada con cadena de overrides JSON/variables de entorno/YAML |
| `radarlib.utils` | Utilidades de nomenclatura de archivos, utilidades de tipo de campo, utilidades de cuadrícula |

---

## Características Principales

- **Pipeline completo de extremo a extremo** — desde la descarga BUFR hasta la entrega
  de productos en un único servicio orquestado.
- **Arquitectura asíncrona** — cada daemon se ejecuta como una coroutine `asyncio`
  para I/O concurrente y no bloqueante.
- **Gestión de estado tolerante a fallos** — `StateTracker` respaldado por SQLite permite
  reanudación automática tras caídas; ningún archivo se procesa dos veces.
- **Decodificación BUFR** — lector BUFR de alto rendimiento que envuelve bibliotecas
  compartidas C/Fortran incluidas con el paquete.
- **Integración con PyART** — lee datos decodificados en objetos `pyart.core.Radar`;
  soporta pipelines de filtrado de campos arbitrarios.
- **Cálculo COLMAX** — reflectividad máxima columnar con límite de elevación configurable
  y filtros de calidad RhoHV/WRAD/ZDR.
- **Modos de salida duales** — genera barridos de radar como imágenes PNG *o* como
  GeoTIFFs optimizados para la nube con georreferencia EPSG:4326 correcta.
- **Mapas de color personalizados** — más de 8 mapas de color sintonizados por GRC para
  reflectividad, velocidad, ZDR, RhoHV, PhiDP, KDP, etc.
- **Configuración flexible** — cadena de override multinivel
  (variables de entorno → archivo JSON → archivo YAML → valores por defecto).
- **Listo para Docker** — `Dockerfile` y `docker-compose.yml` permiten el despliegue
  multi-radar sin cambios de código.
- **Pruebas completas** — 43 archivos de prueba cubriendo escenarios unitarios y de
  integración con `pytest` + `pytest-asyncio`.

---

## Requisitos y Dependencias

### Versión de Python

Se requiere Python **≥ 3.11**, **< 4.0**.

### Dependencias de tiempo de ejecución requeridas

| Paquete | Versión mínima | Propósito |
|---|---|---|
| `arm-pyart` | ≥ 2.1.1 | Modelo de datos y E/S de radar |
| `numpy` | ≥ 2.3.5 | Computación de arreglos |
| `pandas` | ≥ 2.3.3 | Manipulación de datos |
| `xarray` | ≥ 2024.0.0 | Arreglos N-D etiquetados / NetCDF |
| `netcdf4` | ≥ 1.7.0 | Lectura/escritura NetCDF-4 |
| `scipy` | ≥ 1.14.0 | Interpolación y procesamiento de señales |
| `matplotlib` | ≥ 3.9.0 | Motor de gráficos y mapas de color |
| `pillow` | ≥ 10.0.0 | Codificación de imágenes PNG |
| `rasterio` | ≥ 1.3.0 | Lectura/escritura GeoTIFF |
| `GDAL` | = 3.10.3 | Biblioteca geoespacial (requerida por rasterio) |
| `pyproj` | ≥ 3.0.0 | Transformaciones de sistema de referencia de coordenadas |
| `affine` | ≥ 2.0.0 | Utilidades de transformación afín |
| `aioftp` | ≥ 0.22.0 | Cliente FTP asíncrono |
| `cachetools` | ≥ 5.0.0 | Utilidades de caché LRU |
| `pytz` | ≥ 2025.2 | Manejo de zonas horarias |

### Dependencias de desarrollo / pruebas

| Paquete | Propósito |
|---|---|
| `pytest` | Ejecutor de pruebas |
| `pytest-asyncio` | Soporte para pruebas asíncronas |
| `flake8` | Análisis estático de código |
| `black` | Formateo de código |
| `mypy` | Verificación estática de tipos |
| `tox` | Gestión de entornos de prueba |

### Dependencias del sistema (Linux)

Las bibliotecas nativas de GDAL deben estar instaladas:

```bash
apt-get install -y gdal-bin libgdal-dev build-essential
```

---

## Instalación

### Desde el código fuente (desarrollo)

```bash
# 1. Clonar el repositorio
git clone https://github.com/jgmarti84/gl-radarlib.git
cd gl-radarlib

# 2. Crear y activar un entorno virtual (Python ≥ 3.11)
python3 -m venv venv
source venv/bin/activate

# 3. Instalar dependencias del sistema (Debian/Ubuntu)
sudo apt-get install -y gdal-bin libgdal-dev build-essential git

# 4. Instalar el paquete en modo editable con todas las dependencias
pip install -e .

# 5. (Opcional) Instalar dependencias de desarrollo
pip install -r requirements-dev.txt
```

### Usando el Makefile

```bash
make venv       # Crea el entorno virtual
make setup      # Instala todas las dependencias + hooks pre-commit
make test       # Ejecuta las pruebas
make lint       # Ejecuta flake8
```

### Configuración de credenciales y entorno

Proporcione las credenciales FTP ya sea como variables de entorno o a través del
archivo de configuración `genpro25.yml` (ver
[Referencia de Configuración](#referencia-de-configuración)):

```bash
export FTP_HOST="ftp.su-servidor-radar.ejemplo.com"
export FTP_USER="su_usuario"
export FTP_PASS="su_contraseña"
```

---

## Inicio Rápido

El siguiente ejemplo inicia el pipeline completo para un sitio de radar:

```python
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from radarlib.daemons import DaemonManager, DaemonManagerConfig

config = DaemonManagerConfig(
    radar_name="RMA1",
    base_path=Path("/data/radares/RMA1"),
    ftp_host="ftp.ejemplo.com",
    ftp_user="usuario_radar",
    ftp_password="contraseña_radar",
    ftp_base_path="/L2",
    volume_types={
        "0315": {
            "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
            "02": ["VRAD", "WRAD"],
        }
    },
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    product_type="geotiff",
    add_colmax=True,
)

manager = DaemonManager(config)
asyncio.run(manager.start())
```

---

## Referencia de Módulos

### `radarlib.config`

Módulo de configuración centralizado. Carga configuraciones desde (en orden de prioridad):

1. Variable de entorno `RADARLIB_CONFIG` apuntando a un archivo JSON
2. Variables de entorno individuales que coincidan con nombres de claves de configuración
3. Valores por defecto del paquete

#### Atributos públicos

Todos los atributos listados a continuación están disponibles directamente en el módulo
tras la importación:

```python
import radarlib.config as cfg
print(cfg.FTP_HOST)
```

| Atributo | Tipo | Valor por defecto | Descripción |
|---|---|---|---|
| `BUFR_RESOURCES_PATH` | `str` | `<paquete>/io/bufr/bufr_resources` | Ruta a recursos BUFR incluidos |
| `ROOT_CACHE_PATH` | `str` | `~/workspaces/radarlib/cache` | Directorio de caché |
| `ROOT_RADAR_FILES_PATH` | `str` | `~/workspaces/radarlib/data/radares` | Ruta base para archivos de radar crudos |
| `ROOT_RADAR_PRODUCTS_PATH` | `str` | `~/workspaces/radarlib/product_output` | Ruta base para productos generados |
| `ROOT_LOGS_PATH` | `str` | `~/workspaces/radarlib/logs` | Directorio de logs |
| `ROOT_GATE_COORDS_PATH` | `str` | `~/workspaces/radarlib/data/gate_coordinates` | Coordenadas de compuerta precomputadas |
| `ROOT_GEOMETRY_PATH` | `str` | `~/workspaces/radarlib/data/geometries` | Directorio de archivos de geometría |
| `FTP_HOST` | `str` | `"www.example.com"` | Nombre de host del servidor FTP |
| `FTP_USER` | `str` | `"example_user"` | Usuario FTP |
| `FTP_PASS` | `str` | `"secret"` | Contraseña FTP |
| `VOLUME_TYPES` | `dict` | `{"0315": {"01": [...], "02": [...]}}` | Mapeo tipo de volumen BUFR → campos |
| `COLMAX_THRESHOLD` | `float` | `-3` | Reflectividad mínima (dBZ) para COLMAX |
| `COLMAX_ELEV_LIMIT1` | `float` | `0.65` | Elevación mínima (°) incluida en COLMAX |
| `COLMAX_RHOHV_FILTER` | `bool` | `True` | Habilitar filtro de calidad RhoHV para COLMAX |
| `COLMAX_RHOHV_UMBRAL` | `float` | `0.8` | Umbral RhoHV (compuertas por debajo son enmascaradas) |
| `COLMAX_WRAD_FILTER` | `bool` | `True` | Habilitar filtro de calidad por ancho espectral |
| `COLMAX_WRAD_UMBRAL` | `float` | `4.6` | Umbral de ancho espectral (m/s) |
| `COLMAX_TDR_FILTER` | `bool` | `True` | Habilitar filtro de calidad ZDR |
| `COLMAX_TDR_UMBRAL` | `float` | `8.5` | Umbral ZDR (dB) |
| `GRC_RHV_FILTER` | `bool` | `True` | Habilitar filtro de eco de tierra basado en RhoHV |
| `GRC_RHV_THRESHOLD` | `float` | `0.55` | Umbral RhoHV para eco de tierra |
| `GRC_WRAD_FILTER` | `bool` | `True` | Habilitar filtro de eco de tierra por ancho espectral |
| `GRC_WRAD_THRESHOLD` | `float` | `4.6` | Umbral de ancho espectral para eco de tierra |
| `GRC_REFL_FILTER` | `bool` | `True` | Habilitar filtro de baja reflectividad |
| `GRC_REFL_THRESHOLD` | `float` | `-3` | Umbral de baja reflectividad (dBZ) |
| `GRC_ZDR_FILTER` | `bool` | `True` | Habilitar filtro de valores atípicos ZDR |
| `GRC_ZDR_THRESHOLD` | `float` | `8.5` | Umbral de valores atípicos ZDR (dB) |
| `GRC_REFL_FILTER2` | `bool` | `True` | Habilitar segundo filtro de reflectividad |
| `GRC_REFL_THRESHOLD2` | `float` | `25` | Segundo umbral de reflectividad (dBZ) |
| `GRC_CM_FILTER` | `bool` | `True` | Habilitar filtro de consistencia entre momentos |
| `GRC_RHOHV_THRESHOLD2` | `float` | `0.85` | Segundo umbral RhoHV |
| `GRC_DESPECKLE_FILTER` | `bool` | `True` | Habilitar eliminación de ruido puntual |
| `GRC_MEAN_FILTER` | `bool` | `True` | Habilitar suavizado de campo medio |
| `GRC_MEAN_THRESHOLD` | `float` | `0.85` | Umbral de suavizado de campo medio |
| `FIELDS_TO_PLOT` | `list` | `["DBZH", "ZDR", "RHOHV", "COLMAX"]` | Campos renderizados en productos PNG |
| `FILTERED_FIELDS_TO_PLOT` | `list` | `["DBZH", "ZDR", "COLMAX", ...]` | Campos renderizados en productos PNG filtrados |
| `PNG_DPI` | `int` | `72` | Resolución de salida PNG (puntos por pulgada) |
| `GEOMETRY_RES` | `float` | `1200.0` | Resolución de la cuadrícula (metros) |
| `GEOMETRY_TOA` | `float` | `12000.0` | Altura del tope de la atmósfera para geometría (metros) |
| `GEOMETRY_HFAC` | `float` | `0.017` | Factor de altura de geometría |
| `GEOMETRY_MIN_RADIUS` | `float` | `250.0` | Radio mínimo de la compuerta de rango (metros) |
| `GEOMETRY_BUFR_LOOKBACK_HOURS` | `int` | `72` | Horas hacia atrás al buscar archivos de geometría |

#### Funciones

```python
radarlib.config.get(key: str, default: Any = None) -> Any
```

Obtiene un único valor de configuración por nombre.

```python
radarlib.config.reload(path: Optional[str] = None) -> None
```

Fuerza la recarga de toda la configuración. Si se provee `path`, se intenta primero.

---

### `radarlib.daemons`

Procesos trabajadores en segundo plano que forman el pipeline de procesamiento.

#### `DaemonManagerConfig`

Dataclass que agrupa toda la configuración para el despliegue de una sola instancia de radar.

```python
from radarlib.daemons import DaemonManagerConfig

config = DaemonManagerConfig(
    radar_name="RMA1",           # Código del sitio de radar
    base_path=Path("/data/RMA1"),
    ftp_host="ftp.ejemplo.com",
    ftp_user="usuario",
    ftp_password="contraseña",
    ftp_base_path="/L2",
    volume_types={...},
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
)
```

**Parámetros**

| Parámetro | Tipo | Valor por defecto | Descripción |
|---|---|---|---|
| `radar_name` | `str` | requerido | Código del sitio de radar (ej. `"RMA1"`) |
| `base_path` | `Path` | requerido | Directorio raíz para todos los datos del pipeline |
| `ftp_host` | `str` | requerido | Nombre de host del servidor FTP |
| `ftp_user` | `str` | requerido | Usuario FTP |
| `ftp_password` | `str` | requerido | Contraseña FTP |
| `ftp_base_path` | `str` | requerido | Ruta base remota en el servidor FTP |
| `volume_types` | `dict` | requerido | Código de volumen BUFR → código de barrido → lista de campos |
| `start_date` | `datetime` | `now()` UTC | Marca de tiempo más antigua a considerar para descargas |
| `download_poll_interval` | `int` | `60` | Segundos entre ciclos de sondeo FTP |
| `processing_poll_interval` | `int` | `30` | Segundos entre ciclos de procesamiento BUFR→NetCDF |
| `product_poll_interval` | `int` | `30` | Segundos entre ciclos de generación de productos |
| `cleanup_poll_interval` | `int` | `1800` | Segundos entre ciclos de limpieza |
| `enable_download_daemon` | `bool` | `True` | Iniciar el daemon de descarga |
| `enable_processing_daemon` | `bool` | `True` | Iniciar el daemon de procesamiento |
| `enable_product_daemon` | `bool` | `True` | Iniciar el daemon de generación de productos |
| `enable_cleanup_daemon` | `bool` | `False` | Iniciar el daemon de limpieza (deshabilitado por defecto) |
| `product_type` | `str` | `"image"` | Tipo de producto de salida: `"image"` o `"geotiff"` |
| `add_colmax` | `bool` | `True` | Incluir campo COLMAX en la salida de productos |
| `bufr_retention_days` | `int` | `7` | Días antes de que los archivos BUFR sean eliminados |
| `netcdf_retention_days` | `int` | `7` | Días antes de que los archivos NetCDF sean eliminados |
| `product_dir` | `Path \| None` | `base_path/products` | Override del directorio de salida de productos |

#### `DaemonManager`

Orquesta todos los daemons.

```python
from radarlib.daemons import DaemonManager

manager = DaemonManager(config)

# Inicia todos los daemons habilitados (asíncrono)
await manager.start()

# Detiene todos los daemons en ejecución (síncrono)
manager.stop()

# Consulta el estado
status = manager.get_status()
# Retorna dict: {"radar_code", "base_path", "download_daemon": {...}, ...}
```

**Métodos**

| Método | Firma | Descripción |
|---|---|---|
| `__init__` | `(config: DaemonManagerConfig)` | Crea el manager; crea los directorios requeridos |
| `start` | `async () -> None` | Inicia todos los daemons habilitados concurrentemente |
| `stop` | `() -> None` | Detiene todos los daemons en ejecución |
| `get_status` | `() -> dict` | Retorna el diccionario de estado en tiempo de ejecución |

#### `DownloadDaemon`

Sondea un servidor FTP, descubre nuevos archivos BUFR, los verifica con checksum y los
descarga al directorio local `bufr/`. Utiliza `SQLiteStateTracker` para rastrear el
estado de descarga y evitar duplicados.

#### `ProcessingDaemon`

Monitorea el directorio BUFR local en busca de archivos `downloaded`, los decodifica
usando `radarlib.io.bufr`, ensambla volúmenes de radar completos y escribe archivos
NetCDF-4 en el directorio `netcdf/`.

#### `ProductGenerationDaemon`

Monitorea el directorio `netcdf/` en busca de volúmenes `processed`, los carga vía
PyART, aplica filtros de calidad GRC, opcionalmente calcula COLMAX y escribe imágenes
PNG o GeoTIFFs optimizados para la nube en el directorio de productos.

#### `CleanupDaemon`

Periódicamente escanea la base de datos de estado en busca de archivos cuya generación
de productos está completa y cuya antigüedad excede los umbrales de retención configurados.
Elimina los archivos BUFR y NetCDF correspondientes para liberar espacio en disco.

---

### `radarlib.io.bufr`

Decodificador de archivos BUFR de bajo nivel. Envuelve bibliotecas compartidas
Fortran/C incluidas en el sub-paquete `bufr_resources/`.

```python
from radarlib.io.bufr import BufrFile

with BufrFile("/data/RMA1_0315_01_DBZH_20250101T120000Z.BUFR") as bufr:
    data = bufr.read()          # Retorna arreglos numpy estructurados
    metadata = bufr.metadata    # Metadatos del sitio de radar
```

**Clases principales**

| Clase | Descripción |
|---|---|
| `BufrFile` | Envoltura de gestor de contexto para un único archivo BUFR |
| `BufrDecoder` | Decodificador de bajo nivel; llamado internamente por `BufrFile` |
| `BufrVolume` | Agrega múltiples barridos en un volumen completo |

---

### `radarlib.io.ftp`

Cliente FTP asíncrono para recuperación de datos de radar.

```python
from radarlib.io.ftp import FTPClient

async with FTPClient(host="ftp.ejemplo.com", user="u", ******) as ftp:
    files = await ftp.list_files("/L2/RMA1/")
    await ftp.download("/L2/RMA1/archivo.BUFR", "/ruta/local/archivo.BUFR")
```

**Clases principales**

| Clase | Descripción |
|---|---|
| `FTPClient` | Cliente FTP asíncrono con soporte de reintentos y checksum |
| `FTPClientConfig` | Dataclass de configuración para `FTPClient` |

**Características**

- Reintentos con retroceso exponencial ante errores de conexión
- Verificación de checksum MD5 tras la descarga
- Listado de directorios con filtrado por patrón glob

---

### `radarlib.io.pyart`

Capa de integración con PyART: filtrado de campos, cálculo COLMAX y exportación de imágenes.

```python
from radarlib.io.pyart import (
    apply_grc_filters,
    compute_colmax,
    save_png,
    save_geotiff,
)
import pyart

radar = pyart.io.read_cfradial("/data/RMA1/netcdf/volumen.nc")

# Aplicar filtros de calidad
filtrado = apply_grc_filters(radar)

# Calcular reflectividad máxima columnar
colmax = compute_colmax(filtrado)

# Exportar imagen PNG
save_png(radar, field="DBZH", output_path="/productos/DBZH.png")

# Exportar GeoTIFF optimizado para la nube
save_geotiff(colmax, output_path="/productos/COLMAX.tif")
```

**Funciones principales**

| Función | Descripción |
|---|---|
| `apply_grc_filters(radar)` | Aplica filtros RhoHV, WRAD, REFL, ZDR, despeckle y campo medio |
| `compute_colmax(radar, ...)` | Calcula el barrido de reflectividad máxima columnar |
| `save_png(radar, field, ...)` | Renderiza un barrido a una imagen PNG |
| `save_geotiff(grid, ...)` | Escribe una cuadrícula en un GeoTIFF optimizado para la nube |
| `filter_by_rhohv(radar, ...)` | Enmascara compuertas donde RhoHV < umbral |
| `filter_by_wrad(radar, ...)` | Enmascara compuertas donde WRAD > umbral |

---

### `radarlib.radar_grid`

Precomputa geometría de interpolación polar-a-cartesiana para cuadriculado repetido rápido.

```python
from radarlib.radar_grid import RadarGrid

grid = RadarGrid.from_radar(radar, resolution=1200.0)
cartesiano = grid.interpolate(radar.fields["DBZH"]["data"])
```

**Clases principales**

| Clase | Descripción |
|---|---|
| `RadarGrid` | Contiene geometría precomputada; provee el método `interpolate()` |
| `GateGrid` | Contenedor de bajo nivel para coordenadas de compuertas |

---

### `radarlib.state`

Coordinación del pipeline mediante seguimiento persistente del estado.

```python
from radarlib.state import SQLiteStateTracker

tracker = SQLiteStateTracker("/data/estado.db")

# Marcar un archivo como descargado
tracker.set_state("archivo.BUFR", "downloaded")

# Consultar el estado
estado = tracker.get_state("archivo.BUFR")  # ej. "downloaded", "processed", "product_ready"

# Listar todos los archivos en un estado dado
pendientes = tracker.list_by_state("downloaded")
```

**Estados**

| Estado | Significado |
|---|---|
| `"discovered"` | Archivo encontrado en FTP |
| `"downloaded"` | Archivo guardado localmente |
| `"processed"` | BUFR decodificado a NetCDF |
| `"product_ready"` | PNG/GeoTIFF escrito |
| `"cleanup_done"` | Archivos crudos eliminados |

---

### `radarlib.colormaps`

Mapas de color matplotlib personalizados para campos de radar de polarización dual.

```python
import matplotlib.pyplot as plt
import radarlib.colormaps  # registra los mapas de color al importar

# Usar mapas de color por nombre en cualquier llamada matplotlib
plt.pcolormesh(data, cmap="grc_th")
plt.pcolormesh(data, cmap="grc_vrad")
```

**Mapas de color disponibles**

| Nombre | Campo | Descripción |
|---|---|---|
| `grc_th` | Reflectividad | Mapa de color de reflectividad GRC |
| `grc_vrad` | Velocidad radial | Mapa de color de velocidad divergente |
| `grc_rho` | RhoHV | Correlación co-polar |
| `grc_zdr` | ZDR | Reflectividad diferencial |
| `grc_phidp` | PhiDP | Fase diferencial |
| `grc_kdp` | KDP | Fase diferencial específica |
| `grc_wrad` | WRAD | Ancho espectral |
| `grc_cm` | Máscara de ecos de tierra | Campo booleano de ecos de tierra |

---

### `radarlib.utils`

Funciones de utilidad usadas en toda la biblioteca.

**`radarlib.utils.names_utils`**

```python
from radarlib.utils.names_utils import parse_bufr_filename, build_bufr_filename

# Parsear un nombre de archivo BUFR en sus componentes
partes = parse_bufr_filename("RMA1_0315_01_DBZH_20250101T120000Z.BUFR")
# Retorna: {"radar": "RMA1", "vol_code": "0315", "sweep": "01",
#           "field": "DBZH", "timestamp": datetime(...)}

# Construir un nombre de archivo BUFR a partir de sus componentes
nombre = build_bufr_filename(radar="RMA1", vol_code="0315", sweep="01",
                             field="DBZH", timestamp=dt)
```

**`radarlib.utils.fields_utils`**

```python
from radarlib.utils.fields_utils import get_field_type, is_dual_pol_field

tipo_campo = get_field_type("DBZH")    # retorna "reflectivity"
es_dp = is_dual_pol_field("ZDR")      # retorna True
```

---

## Ejemplos de Uso

### Ejemplo 1: Ejecutar el pipeline completo para un único radar

```python
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from radarlib.daemons import DaemonManager, DaemonManagerConfig

config = DaemonManagerConfig(
    radar_name="RMA1",
    base_path=Path("/data/radares/RMA1"),
    ftp_host="200.16.116.24",
    ftp_user="usuario_radar",
    ftp_password="secreto",
    ftp_base_path="/L2",
    volume_types={
        "0315": {
            "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
            "02": ["VRAD", "WRAD"],
        }
    },
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    product_type="geotiff",
    add_colmax=True,
    enable_cleanup_daemon=True,
    bufr_retention_days=1,
    netcdf_retention_days=1,
)

manager = DaemonManager(config)

try:
    asyncio.run(manager.start())
except KeyboardInterrupt:
    manager.stop()
```

---

### Ejemplo 2: Decodificar un archivo BUFR y leer metadatos

```python
from radarlib.io.bufr import BufrFile

ruta = "/data/RMA1/bufr/RMA1_0315_01_DBZH_20250101T120000Z.BUFR"

with BufrFile(ruta) as bufr:
    metadata = bufr.metadata
    print(f"Radar: {metadata['radar_name']}")
    print(f"Latitud/Longitud: {metadata['latitude']}, {metadata['longitude']}")
    print(f"Elevación: {metadata['elevation_angle']}°")

    data = bufr.read()
    print(f"Forma de los datos: {data.shape}")
    print(f"Compuertas de rango: {data.shape[1]}, Bins azimutal: {data.shape[0]}")
```

---

### Ejemplo 3: Descargar archivos desde FTP

```python
import asyncio
from radarlib.io.ftp import FTPClient

async def descargar_recientes(radar_name: str):
    async with FTPClient(
        host="ftp.ejemplo.com",
        user="usuario_radar",
        ******,
    ) as ftp:
        ruta_remota = f"/L2/{radar_name}/"
        archivos = await ftp.list_files(ruta_remota)

        for archivo_remoto in archivos[-5:]:   # últimos 5 archivos
            ruta_local = f"/data/{radar_name}/bufr/{archivo_remoto.split('/')[-1]}"
            await ftp.download(archivo_remoto, ruta_local)
            print(f"Descargado: {ruta_local}")

asyncio.run(descargar_recientes("RMA1"))
```

---

### Ejemplo 4: Aplicar filtros y exportar una imagen PNG

```python
import pyart
from radarlib.io.pyart import apply_grc_filters, save_png

# Cargar un volumen NetCDF producido por ProcessingDaemon
radar = pyart.io.read_cfradial("/data/RMA1/netcdf/20250101T120000Z.nc")

# Aplicar filtros de calidad GRC (RhoHV, WRAD, REFL, ZDR, despeckle, campo medio)
radar_filtrado = apply_grc_filters(radar)

# Renderizar el primer barrido de DBZH a PNG
save_png(
    radar_filtrado,
    field="DBZH",
    sweep_index=0,
    output_path="/productos/RMA1/DBZH_20250101T120000Z.png",
    vmin=-20,
    vmax=70,
    cmap="grc_th",
    dpi=72,
)
print("PNG exportado exitosamente.")
```

---

### Ejemplo 5: Calcular y exportar un GeoTIFF COLMAX optimizado para la nube

```python
import pyart
from radarlib.io.pyart import apply_grc_filters, compute_colmax, save_geotiff

radar = pyart.io.read_cfradial("/data/RMA1/netcdf/20250101T120000Z.nc")
radar_filtrado = apply_grc_filters(radar)

# Calcular reflectividad máxima columnar
grilla_colmax = compute_colmax(
    radar_filtrado,
    threshold=-3.0,       # dBZ mínimo incluido
    elev_limit=0.65,      # elevación mínima (grados)
)

# Escribir GeoTIFF optimizado para la nube (EPSG:4326)
save_geotiff(
    grilla_colmax,
    output_path="/productos/RMA1/COLMAX_20250101T120000Z.tif",
    crs="EPSG:4326",
)
print("COG exportado exitosamente.")
```

---

### Ejemplo 6: Usar mapas de color personalizados en matplotlib

```python
import numpy as np
import matplotlib.pyplot as plt
import radarlib.colormaps   # registra todos los mapas de color GRC

# Datos de reflectividad simulados
data = np.random.uniform(-20, 70, (360, 240))

fig, ax = plt.subplots(figsize=(8, 8))
mesh = ax.pcolormesh(data, cmap="grc_th", vmin=-20, vmax=70)
plt.colorbar(mesh, ax=ax, label="Reflectividad (dBZ)")
ax.set_title("DBZH Simulado")
plt.savefig("reflectividad.png", dpi=150)
```

---

## Referencia de Configuración

### Variables de entorno

Las siguientes variables de entorno son reconocidas por `radarlib.config` y
pueden usarse para anular los valores por defecto. También son utilizadas por
`app/config.py` al desplegar vía Docker Compose.

| Variable | Tipo | Valor por defecto | Descripción |
|---|---|---|---|
| `RADARLIB_CONFIG` | ruta | — | Ruta a un archivo JSON de configuración que anula los valores por defecto |
| `ROOT_CACHE_PATH` | ruta | ver arriba | Directorio de caché |
| `ROOT_RADAR_FILES_PATH` | ruta | ver arriba | Raíz para archivos de radar crudos |
| `ROOT_RADAR_PRODUCTS_PATH` | ruta | ver arriba | Raíz para productos generados |
| `ROOT_LOGS_PATH` | ruta | ver arriba | Directorio de salida de logs |
| `ROOT_GATE_COORDS_PATH` | ruta | ver arriba | Archivos de coordenadas de compuertas |
| `FTP_HOST` | cadena | `"www.example.com"` | Nombre de host del servidor FTP |
| `FTP_USER` | cadena | `"example_user"` | Usuario FTP |
| `FTP_PASS` | cadena | `"secret"` | Contraseña FTP |
| `COLMAX_THRESHOLD` | flotante | `-3` | Reflectividad mínima COLMAX (dBZ) |
| `COLMAX_ELEV_LIMIT1` | flotante | `0.65` | Elevación mínima COLMAX (°) |
| `GRC_RHV_THRESHOLD` | flotante | `0.55` | Umbral del filtro RhoHV |
| `GRC_WRAD_THRESHOLD` | flotante | `4.6` | Umbral del filtro de ancho espectral |
| `PNG_DPI` | entero | `72` | Resolución de salida PNG |

### Archivo de configuración JSON

Apunte `RADARLIB_CONFIG` a un archivo JSON para anulaciones masivas:

```json
{
  "FTP_HOST": "ftp.mi-radar.ejemplo.com",
  "FTP_USER": "radar",
  "FTP_PASS": "mi_secreto",
  "PNG_DPI": 100,
  "COLMAX_THRESHOLD": -5
}
```

### Configuración YAML de la aplicación (`genpro25.yml`)

La capa de aplicación usa un archivo YAML multi-entorno. Seleccione el entorno
activo con `GENPRO25_ENV` (por defecto: `local`).

```yaml
local:
  FTP:
    FTP_HOST: "ftp.ejemplo.com"
    FTP_USER: "usuario_radar"
    FTP_PASS: "secreto"
  DAEMON_PARAMS:
    START_DATE: "2025-01-01T00:00:00Z"
    ENABLE_DOWNLOAD_DAEMON: true
    ENABLE_PROCESSING_DAEMON: true
    ENABLE_PRODUCT_DAEMON: true
    ENABLE_CLEANUP_DAEMON: false
    DOWNLOAD_POLL_INTERVAL: 60
    PROCESSING_POLL_INTERVAL: 30
    PRODUCT_POLL_INTERVAL: 30
    CLEANUP_POLL_INTERVAL: 1800
    PRODUCT_TYPE: "geotiff"
    ADD_COLMAX: true
    NETCDF_RETENTION_DAYS: 7
    BUFR_RETENTION_DAYS: 7
```

---

## Manejo de Errores

### Excepciones comunes

| Excepción | Módulo | Causa | Manejo |
|---|---|---|---|
| `FileNotFoundError` | `app/config.py` | La ruta de `GENPRO25_CONFIG` no existe | Asegúrese de que el YAML de configuración esté montado correctamente |
| `DownloadDaemonError` | `radarlib.daemons` | Error FTP irrecuperable | El daemon registra el error y reintenta en el próximo ciclo |
| `ValueError` | `DaemonManagerConfig` | `start_date` no tiene zona horaria | Siempre pase `tzinfo=timezone.utc` |
| `ConnectionRefusedError` | `radarlib.io.ftp` | Servidor FTP inalcanzable | `FTPClient` reintenta con retroceso exponencial |
| `OSError` | `radarlib.io.bufr` | Archivo BUFR corrupto o incompleto | El archivo es marcado como `"failed"` en la BD de estado; el pipeline continúa |

### Mejores prácticas

```python
import asyncio
from radarlib.daemons import DaemonManager

manager = DaemonManager(config)
try:
    asyncio.run(manager.start())
except KeyboardInterrupt:
    # Apagado ordenado con Ctrl-C
    manager.stop()
except Exception as exc:
    # Registrar errores inesperados y detener limpiamente
    import logging
    logging.exception("Error fatal: %s", exc)
    manager.stop()
    raise
```

---

---

## Guía de Despliegue

### Descripción General

`radarlib` se despliega como un **contenedor Docker sin estado por instancia de radar**.
Cada contenedor ejecuta `app/main.py`, que:

1. Lee `genpro25.yml` (montado como volumen de solo lectura)
2. Fusiona la configuración YAML con los valores por defecto de `radarlib` y las
   variables de entorno
3. Construye un `DaemonManager` e inicia todos los daemons habilitados asincrónicamente

Una única imagen Docker puede servir a cualquier número de sitios de radar cambiando
la variable de entorno `RADAR_NAME` y las credenciales FTP.

### Despliegue con Docker Compose

#### Prerequisitos

- Docker Engine ≥ 24.0 y Docker Compose ≥ 2.20
- Bibliotecas nativas GDAL 3.10.3 (instaladas en la imagen)
- Credenciales FTP válidas para la red de radar objetivo
- Un archivo de configuración `genpro25.yml` correctamente configurado

#### Ejecutar una instancia de radar

```bash
# Iniciar todos los servicios definidos en docker-compose.yml
docker compose up -d

# Seguir los logs de una instancia específica
docker compose logs -f genpro25
```

#### Detener y reiniciar

```bash
# Detener todos los contenedores (los datos se preservan en los volúmenes)
docker compose down

# Reiniciar un único servicio
docker compose restart genpro25

# Obtener una nueva imagen y recrear los contenedores
docker compose pull && docker compose up -d
```

---

### Referencia Completa de Configuración

Las siguientes variables son utilizadas por `app/main.py` y `app/config.py`.

---

#### `RADAR_NAME`

- **Descripción**: Identifica el sitio de radar. Usado para construir rutas de archivos, directorios de logs y rutas FTP remotas.
- **Tipo**: cadena
- **Requerido**: No
- **Valor por defecto**: `"RMA2"`
- **Valor de ejemplo**: `RADAR_NAME=RMA1`
- **Dónde configurarlo**: Sección `environment:` de `docker-compose.yml`
- **Consumido por**: `app/main.py`, `DaemonManagerConfig.radar_name`, formateador de logs
- **Notas**: Debe coincidir con el código de radar usado en los nombres de archivos BUFR y en el servidor FTP.

---

#### `GENPRO25_CONFIG`

- **Descripción**: Ruta absoluta al archivo de configuración YAML `genpro25.yml` dentro del contenedor.
- **Tipo**: ruta
- **Requerido**: No
- **Valor por defecto**: `"/workspace/app/genpro25.yml"`
- **Valor de ejemplo**: `GENPRO25_CONFIG=/workspace/app/genpro25.yml`
- **Dónde configurarlo**: Sección `environment:` de `docker-compose.yml`
- **Consumido por**: `app/config.py` al arrancar
- **Notas**: El archivo debe estar montado como volumen (ver [Referencia de Volúmenes y Montajes](#referencia-de-volúmenes-y-montajes)).

---

#### `GENPRO25_ENV`

- **Descripción**: Selecciona el bloque de entorno (`local`, `stg`, `prd`) dentro de `genpro25.yml`.
- **Tipo**: cadena
- **Requerido**: No
- **Valor por defecto**: `"local"`
- **Valor de ejemplo**: `GENPRO25_ENV=prd`
- **Dónde configurarlo**: Sección `environment:` de `docker-compose.yml`
- **Consumido por**: `app/config.py` (`_GENPRO25_ENV`)
- **Notas**: La clave elegida debe existir como clave de nivel superior en `genpro25.yml`.

---

#### `ROOT_CACHE_PATH`

- **Descripción**: Directorio usado para almacenar datos intermedios en caché.
- **Tipo**: ruta
- **Requerido**: No
- **Valor por defecto**: `"/workspace/app/cache"` (mediante override de env en docker-compose)
- **Valor de ejemplo**: `ROOT_CACHE_PATH=/workspace/app/cache`
- **Dónde configurarlo**: Sección `environment:` de `docker-compose.yml`
- **Consumido por**: `radarlib.config`, varios daemons
- **Notas**: Creado automáticamente por el contenedor si no existe.

---

#### `ROOT_RADAR_FILES_PATH`

- **Descripción**: Directorio raíz donde se almacenan los archivos de radar crudos (BUFR, NetCDF), organizados por nombre de radar.
- **Tipo**: ruta
- **Requerido**: No
- **Valor por defecto**: `"/workspace/app/data/radares"`
- **Valor de ejemplo**: `ROOT_RADAR_FILES_PATH=/workspace/app/data/radares`
- **Dónde configurarlo**: Sección `environment:` de `docker-compose.yml`
- **Consumido por**: `app/main.py` → `DaemonManagerConfig.base_path`
- **Notas**: Cada radar obtiene un sub-directorio: `<ROOT_RADAR_FILES_PATH>/<RADAR_NAME>/`.

---

#### `ROOT_RADAR_PRODUCTS_PATH`

- **Descripción**: Directorio raíz donde se escriben los productos generados (PNG / GeoTIFF).
- **Tipo**: ruta
- **Requerido**: No
- **Valor por defecto**: `"/workspace/app/product_output"`
- **Valor de ejemplo**: `ROOT_RADAR_PRODUCTS_PATH=/workspace/app/product_output`
- **Dónde configurarlo**: Sección `environment:` de `docker-compose.yml`
- **Consumido por**: `app/main.py` → `DaemonManagerConfig.product_dir`
- **Notas**: Debe montarse como bind mount a una ruta del host para preservar productos entre reinicios.

---

#### `ROOT_GATE_COORDS_PATH`

- **Descripción**: Directorio que contiene archivos de coordenadas de compuertas precomputadas para cuadriculado rápido.
- **Tipo**: ruta
- **Requerido**: No
- **Valor por defecto**: `"/workspace/app/data/gate_coordinates"`
- **Valor de ejemplo**: `ROOT_GATE_COORDS_PATH=/workspace/app/data/gate_coordinates`
- **Dónde configurarlo**: Sección `environment:` de `docker-compose.yml`
- **Consumido por**: `radarlib.radar_grid`

---

#### `ROOT_LOGS_PATH`

- **Descripción**: Directorio raíz de logs. Se crea automáticamente un sub-directorio por radar.
- **Tipo**: ruta
- **Requerido**: No
- **Valor por defecto**: `"/workspace/app/logs"`
- **Valor de ejemplo**: `ROOT_LOGS_PATH=/workspace/app/logs`
- **Dónde configurarlo**: Sección `environment:` de `docker-compose.yml`
- **Consumido por**: `app/main.py` (manejador de archivo de log)
- **Notas**: La ruta real del archivo de log es `<ROOT_LOGS_PATH>/<RADAR_NAME>/genpro25.log`. Rotado diariamente; se conservan 7 días.

---

#### `PYTHONDONTWRITEBYTECODE`

- **Descripción**: Impide que Python cree archivos de bytecode `.pyc`.
- **Tipo**: cadena (`"1"` para habilitar)
- **Requerido**: No
- **Valor por defecto**: `"1"` (configurado en Dockerfile y docker-compose)
- **Dónde configurarlo**: `ENV` del Dockerfile o `environment:` de `docker-compose.yml`

---

#### `PYTHONUNBUFFERED`

- **Descripción**: Fuerza que stdout/stderr de Python no tenga buffer, asegurando que los logs aparezcan inmediatamente en `docker logs`.
- **Tipo**: cadena (`"1"` para habilitar)
- **Requerido**: No
- **Valor por defecto**: `"1"` (configurado en Dockerfile y docker-compose)
- **Dónde configurarlo**: `ENV` del Dockerfile o `environment:` de `docker-compose.yml`

---

#### Variables de configuración provenientes de `genpro25.yml` / `DAEMON_PARAMS`

Estos valores se leen de `genpro25.yml` bajo `DAEMON_PARAMS` y anulan los valores
por defecto de `radarlib`.

| Variable | Tipo | Valor por defecto | Descripción |
|---|---|---|---|
| `START_DATE` | cadena ISO-8601 | tiempo UTC actual | Marca de tiempo más antigua para descarga de archivos |
| `ENABLE_DOWNLOAD_DAEMON` | bool | `true` | Habilitar el daemon de descarga |
| `ENABLE_PROCESSING_DAEMON` | bool | `true` | Habilitar el daemon de procesamiento |
| `ENABLE_PRODUCT_DAEMON` | bool | `true` | Habilitar el daemon de generación de productos |
| `ENABLE_CLEANUP_DAEMON` | bool | `true` | Habilitar el daemon de limpieza |
| `DOWNLOAD_POLL_INTERVAL` | entero (segundos) | `60` | Intervalo de sondeo FTP |
| `PROCESSING_POLL_INTERVAL` | entero (segundos) | `30` | Intervalo de procesamiento BUFR→NetCDF |
| `PRODUCT_POLL_INTERVAL` | entero (segundos) | `30` | Intervalo de generación de productos |
| `CLEANUP_POLL_INTERVAL` | entero (segundos) | `1800` | Intervalo de ciclo de limpieza |
| `PRODUCT_TYPE` | cadena | `"geotiff"` | `"image"` o `"geotiff"` |
| `ADD_COLMAX` | bool | `true` | Calcular e incluir COLMAX |
| `NETCDF_RETENTION_DAYS` | flotante (días) | `0.0833` | Período de retención de archivos NetCDF |
| `BUFR_RETENTION_DAYS` | flotante (días) | `0.0833` | Período de retención de archivos BUFR |
| `GEOMETRY_BUFR_LOOKBACK_HOURS` | entero | `72` | Horas hacia atrás para buscar archivos BUFR de geometría |

---

### Precedencia de Configuración

Los valores de configuración se resuelven en el siguiente orden (mayor → menor prioridad):

| Prioridad | Fuente | Cómo usar |
|---|---|---|
| 1 | **`environment:` en `docker-compose.yml`** | Configure o anule cualquier clave de configuración de `radarlib` directamente como variable de entorno. Tiene efecto inmediato al (re)iniciar el contenedor. |
| 2 | **Bloque de entorno `GENPRO25_ENV` en `genpro25.yml`** | Anule configuraciones específicas por entorno (`local`, `stg`, `prd`). Monte el archivo y configure `GENPRO25_ENV`. |
| 3 | **Sección `DAEMON_PARAMS` de `genpro25.yml`** | Anulaciones específicas de daemons (intervalos de sondeo, retención, tipo de producto). |
| 4 | **Archivo JSON de `RADARLIB_CONFIG`** | Proporcione un diccionario JSON con cualquier clave de `radarlib.config`. Señalado por la variable de entorno `RADARLIB_CONFIG`. |
| 5 | **Valores por defecto de `radarlib`** (diccionario `DEFAULTS` en `radarlib/config.py`) | Valores de respaldo codificados. Cambie sólo si parchea la biblioteca. |

**Recomendación**: Use las variables de entorno de `docker-compose.yml` para valores
específicos del despliegue (`RADAR_NAME`, rutas, credenciales FTP) y `genpro25.yml`
para parámetros de procesamiento (umbrales, intervalos, tipo de producto).

---

### Referencia de Volúmenes y Montajes

| Volumen / Bind Mount | Ruta del host | Ruta en el contenedor | Modo | Requerido | Propósito |
|---|---|---|---|---|---|
| Salida de productos | `../product_output` | `/workspace/app/product_output` | lectura-escritura | Sí | Preserva productos PNG / GeoTIFF generados |
| Logs | `../logs` | `/workspace/app/logs` | lectura-escritura | Sí | Preserva archivos de log con rotación diaria |
| Configuración YAML | `./genpro25.yml` | `/workspace/app/genpro25.yml` | solo lectura | Sí | Archivo de configuración de la aplicación |

> **Nota**: Los archivos intermedios BUFR y NetCDF se almacenan dentro del contenedor
> en `ROOT_RADAR_FILES_PATH`. Por defecto no se persisten en el host. Si los necesita
> para depuración, agregue un bind mount en `docker-compose.yml`.

---

### Configuración de Secretos y Credenciales

Las credenciales FTP son los únicos secretos requeridos por el despliegue actual.

#### Desarrollo (docker-compose local)

Para desarrollo es aceptable almacenar las credenciales directamente en
`genpro25.yml` bajo la sección `FTP:`. **No comprometa credenciales reales en el
control de versiones.**

```yaml
local:
  FTP:
    FTP_HOST: "ftp.ejemplo.com"
    FTP_USER: "usuario_radar"
    FTP_PASS: "mi_secreto"
```

#### Producción (recomendado)

En producción, pase las credenciales mediante variables de entorno para que no
estén almacenadas en disco dentro de la imagen:

```yaml
# docker-compose.prod.yml
services:
  genpro25:
    environment:
      FTP_HOST: "${FTP_HOST}"
      FTP_USER: "${FTP_USER}"
      FTP_PASS: "${FTP_PASS}"
```

Proporcione los valores en un archivo `.env` (no comprometido) o mediante su
gestor de secretos CI/CD:

```bash
# .env  (ignorado por git)
FTP_HOST=ftp.ejemplo.com
FTP_USER=usuario_radar
FTP_PASS=mi_secreto
```

> ⚠️ **Nunca codifique las credenciales en `docker-compose.yml` ni las comprometa en
> el control de versiones.** Use archivos `.env`, Docker secrets, o un gestor de
> secretos (Vault, AWS Secrets Manager, etc.) en producción.

---

### Despliegue para un Nuevo Sitio de Radar

#### Paso a paso

1. **Actualice `RADAR_NAME`** en `docker-compose.yml` con el código del nuevo radar
   (ej. `RMA5`).

2. **Actualice las credenciales FTP** si el nuevo radar usa un servidor FTP diferente.

3. **Actualice `genpro25.yml`** sección `VOLUME_TYPES` con los códigos de volumen BUFR
   y las listas de campos para el nuevo radar.

4. **Configure `START_DATE`** en `DAEMON_PARAMS` de `genpro25.yml` con la marca de
   tiempo más antigua a procesar.

5. **Inicie el contenedor**:

   ```bash
   docker compose up -d
   docker compose logs -f genpro25
   ```

#### Fragmento mínimo de `docker-compose.yml` para un nuevo radar

```yaml
services:
  genpro25-rma5:
    build:
      context: .
      dockerfile: ./app/Dockerfile
    container_name: genpro25-rma5
    volumes:
      - ../product_output:/workspace/app/product_output
      - ../logs:/workspace/app/logs
      - ./genpro25.yml:/workspace/app/genpro25.yml:ro
    environment:
      RADAR_NAME: "RMA5"
      GENPRO25_CONFIG: "/workspace/app/genpro25.yml"
      ROOT_CACHE_PATH: "/workspace/app/cache"
      ROOT_RADAR_FILES_PATH: "/workspace/app/data/radares"
      ROOT_RADAR_PRODUCTS_PATH: "/workspace/app/product_output"
      ROOT_GATE_COORDS_PATH: "/workspace/app/data/gate_coordinates"
      ROOT_LOGS_PATH: "/workspace/app/logs"
      FTP_HOST: "ftp.rma5.ejemplo.com"
      FTP_USER: "usuario_rma5"
      FTP_PASS: "contraseña_rma5"
    working_dir: /workspace/app
    command: ["bash", "-c", "mkdir -p /workspace/app/product_output && python main.py"]
    restart: unless-stopped
```

#### Convenciones de nomenclatura

Los nombres de archivos BUFR siguen el patrón:

```
<RADAR_NAME>_<COD_VOL>_<NR_BARRIDO>_<CAMPO>_<TIMESTAMP>.BUFR
ej.  RMA1_0315_01_DBZH_20250101T120000Z.BUFR
```

Los productos se escriben en:

```
<ROOT_RADAR_PRODUCTS_PATH>/<RADAR_NAME>/<CAMPO>_<TIMESTAMP>.<ext>
```

#### Verificar el despliegue

```bash
# Confirmar que el contenedor está en ejecución
docker ps | grep genpro25

# Verificar los logs de arranque
docker compose logs genpro25 | head -30

# La salida esperada incluye:
#   Genpro25 Radar Data Processing Service Starting
#   Starting daemon manager...
#   Both download and processing daemons will start
```

---

### Ejecutar Múltiples Instancias de Radar

Agregue un bloque de servicio por radar en `docker-compose.yml`, variando
`RADAR_NAME`, `container_name` y (si es necesario) `FTP_HOST`/`FTP_USER`/`FTP_PASS`.
Todos los servicios pueden compartir la misma imagen, volumen de logs y volumen de productos.

```yaml
services:
  genpro25-rma1:
    build: { context: ., dockerfile: ./app/Dockerfile }
    container_name: genpro25-rma1
    volumes:
      - ../product_output:/workspace/app/product_output
      - ../logs:/workspace/app/logs
      - ./genpro25.yml:/workspace/app/genpro25.yml:ro
    environment:
      RADAR_NAME: "RMA1"
      # ... otras variables de entorno ...
    restart: unless-stopped

  genpro25-rma3:
    build: { context: ., dockerfile: ./app/Dockerfile }
    container_name: genpro25-rma3
    volumes:
      - ../product_output:/workspace/app/product_output
      - ../logs:/workspace/app/logs
      - ./genpro25.yml:/workspace/app/genpro25.yml:ro
    environment:
      RADAR_NAME: "RMA3"
      # ... otras variables de entorno ...
    restart: unless-stopped
```

Cada servicio escribe en su propio sub-directorio bajo `ROOT_RADAR_FILES_PATH`
y `ROOT_RADAR_PRODUCTS_PATH` (con clave `RADAR_NAME`), por lo que no hay
conflictos entre instancias.

---

### Referencia de Argumentos de Construcción

El `app/Dockerfile` actualmente no tiene directivas `ARG`; toda la configuración se
suministra en tiempo de ejecución mediante variables de entorno. La imagen base y la
versión de GDAL están fijadas en las capas `FROM` y `RUN`:

| Capa | Valor | Descripción |
|---|---|---|
| Imagen base | `python:3.11-slim` | Imagen Python 3.11 mínima basada en Debian |
| Versión GDAL | `3.10.3` (de `requirements.txt`) | Debe coincidir con el `libgdal-dev` nativo instalado en la imagen |

Para reconstruir tras una actualización de dependencias:

```bash
docker compose build --no-cache
```

---

### Despliegue por Entorno

#### Desarrollo (local)

```yaml
# docker-compose.yml — desarrollo local
services:
  genpro25:
    build:
      context: .
      dockerfile: ./app/Dockerfile
    container_name: genpro25
    volumes:
      - ../product_output:/workspace/app/product_output
      - ../logs:/workspace/app/logs
      - ./genpro25.yml:/workspace/app/genpro25.yml:ro
    environment:
      RADAR_NAME: "RMA1"
      GENPRO25_ENV: "local"
      GENPRO25_CONFIG: "/workspace/app/genpro25.yml"
      ROOT_CACHE_PATH: "/workspace/app/cache"
      ROOT_RADAR_FILES_PATH: "/workspace/app/data/radares"
      ROOT_RADAR_PRODUCTS_PATH: "/workspace/app/product_output"
      ROOT_GATE_COORDS_PATH: "/workspace/app/data/gate_coordinates"
      ROOT_LOGS_PATH: "/workspace/app/logs"
    working_dir: /workspace/app
    command: ["bash", "-c", "mkdir -p /workspace/app/product_output && python main.py"]
    restart: "no"
```

#### Producción

```yaml
# docker-compose.prod.yml
services:
  genpro25:
    image: grc/radarlib:latest   # imagen pre-construida del registro
    container_name: genpro25
    volumes:
      - /opt/radar/product_output:/workspace/app/product_output
      - /opt/radar/logs:/workspace/app/logs
      - /opt/radar/config/genpro25.yml:/workspace/app/genpro25.yml:ro
    environment:
      RADAR_NAME: "RMA1"
      GENPRO25_ENV: "prd"
      GENPRO25_CONFIG: "/workspace/app/genpro25.yml"
      ROOT_CACHE_PATH: "/workspace/app/cache"
      ROOT_RADAR_FILES_PATH: "/workspace/app/data/radares"
      ROOT_RADAR_PRODUCTS_PATH: "/workspace/app/product_output"
      ROOT_GATE_COORDS_PATH: "/workspace/app/data/gate_coordinates"
      ROOT_LOGS_PATH: "/workspace/app/logs"
      FTP_HOST: "${FTP_HOST}"
      FTP_USER: "${FTP_USER}"
      FTP_PASS: "${FTP_PASS}"
    restart: unless-stopped
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: "4G"
```

**Lista de verificación de seguridad para producción**

- [ ] Credenciales FTP provistas via archivo `.env` o gestor de secretos (no en YAML)
- [ ] `genpro25.yml` no contiene credenciales reales
- [ ] Directorios de productos y logs montados desde el host o volumen persistente
- [ ] El contenedor se ejecuta como usuario no root (agregue `user: "1000:1000"` si es necesario)
- [ ] Acceso a la red restringido solo al servidor FTP

---

### Verificaciones de Salud y Monitoreo

El `docker-compose.yml` actual no define un `healthcheck` explícito.
Para agregar uno, añada a la definición del servicio:

```yaml
healthcheck:
  test: ["CMD", "python", "-c", "import radarlib; print('ok')"]
  interval: 60s
  timeout: 10s
  retries: 3
  start_period: 30s
```

#### Mensajes clave de log al arrancar

```
======================================================
Genpro25 Radar Data Processing Service Starting
======================================================
Starting daemon manager...
Both download and processing daemons will start
Press Ctrl+C to stop all daemons
```

Si estas líneas aparecen, el servicio inició exitosamente.

#### Verificar el estado del contenedor

```bash
# Verificar el estado de ejecución
docker ps

# Transmitir logs
docker compose logs -f genpro25

# Inspeccionar el código de salida de un contenedor detenido
docker inspect genpro25 --format "{{.State.ExitCode}}"
```

---

### Solución de Problemas

#### El contenedor se cierra inmediatamente

**Causa**: `genpro25.yml` no encontrado en la ruta especificada por `GENPRO25_CONFIG`.

**Solución**: Verifique el montaje de volumen en `docker-compose.yml`:
```yaml
volumes:
  - ./genpro25.yml:/workspace/app/genpro25.yml:ro
```
Y que `./genpro25.yml` exista en el host.

---

#### `FileNotFoundError: Configuration file not found`

**Causa**: `GENPRO25_CONFIG` apunta a una ruta no existente dentro del contenedor.

**Solución**: Verifique el montaje de volumen y que `GENPRO25_CONFIG` coincida exactamente con la ruta en el contenedor.

---

#### Conexión FTP rechazada / tiempo de espera agotado

**Causa**: `FTP_HOST`, `FTP_USER` o `FTP_PASS` incorrectos; firewall de red; servidor FTP caído.

**Solución**:
1. Verifique las credenciales en `genpro25.yml` o variables de entorno.
2. Pruebe la conectividad: `docker exec genpro25 nc -zv <FTP_HOST> 21`
3. Verifique las reglas de firewall entre el contenedor y el servidor FTP.

---

#### `ValueError: start_date must be timezone-aware`

**Causa**: `START_DATE` en `genpro25.yml` no es un timestamp UTC ISO-8601 válido.

**Solución**: Use el formato `"2025-01-01T00:00:00Z"` (la `Z` final indica UTC).

---

#### Los productos no aparecen en el directorio de salida

**Causa**: `ENABLE_PRODUCT_DAEMON` es `false`, o el montaje del volumen de productos es incorrecto.

**Solución**:
1. Verifique `genpro25.yml` `DAEMON_PARAMS.ENABLE_PRODUCT_DAEMON: true`.
2. Verifique el montaje de volumen: `../product_output:/workspace/app/product_output`.
3. Inspeccione los logs para errores de `ProductGenerationDaemon`: `docker compose logs genpro25 | grep product`.

---

#### Alto uso de disco

**Causa**: Daemon de limpieza deshabilitado o umbrales de retención demasiado grandes.

**Solución**: Habilite el daemon de limpieza y reduzca la retención:
```yaml
DAEMON_PARAMS:
  ENABLE_CLEANUP_DAEMON: true
  NETCDF_RETENTION_DAYS: 1
  BUFR_RETENTION_DAYS: 1
```

---

#### Inspeccionar los logs

```bash
# Transmitir todos los logs
docker compose logs -f

# Transmitir logs de un único servicio
docker compose logs -f genpro25

# Últimas 100 líneas
docker compose logs --tail=100 genpro25

# Archivos de log en el host (si están montados)
tail -f ../logs/RMA1/genpro25.log
```
