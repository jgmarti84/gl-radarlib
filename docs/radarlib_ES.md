# radarlib — Documentación Completa

---

## Tabla de Contenidos

### Guía para Operadores y Administradores
1. [Descripción General & Inicio Rápido](#ch1-description)
2. [Instalación & Configuración](#ch2-install)
3. [Referencias de Configuración](#ch3-config)
4. [Despliegue & Operaciones](#ch4-deployment)

### Guía para Desarrolladores
5. [Arquitectura Profunda](#ch5-architecture)
6. [Referencia de Módulos](#ch6-modules)
7. [Guía de Procesamiento BUFR](#ch7-bufr)
8. [Integración & Ejemplos Avanzados](#ch8-integration)

---

# 1. Descripción General & Inicio Rápido {#ch1-description}

## ¿Qué es radarlib?

**radarlib** es una biblioteca Python profesional para la obtención, procesamiento y visualización de datos de radar meteorológico. Está desarrollada y mantenida por el **Grupo Radar Córdoba (GRC)** y está diseñada para servir tanto a sistemas operativos como a flujos de trabajo de investigación.

### El Problema que Resuelve

Las redes de radar meteorológico producen continuamente grandes volúmenes de datos binarios en formatos propietarios o especializados (BUFR, NetCDF, IRIS/SIGMET, etc.). Convertir escaneos de radar sin procesar en productos accionables — mapas PNG georeferenciados o rásters GeoTIFF optimizados en la nube adecuados para servicios posteriores — requiere un pipeline confiable y automatizado. **radarlib** proporciona exactamente ese pipeline.

### Descripción General del Pipeline de Datos



Servidor FTP (archivos BUFR sin procesar)
    |
    | BUFR format
    |
    v
[DownloadDaemon]  -- Monitorea, checksums, reintentos
    |
    | BD SQLite de estado
    |
    v
[ProcessingDaemon]  -- Decodifica BUFR -> volúmenes NetCDF
    |
    | BD SQLite de estado
    |
    v
[ProductGenerationDaemon]  -- Renderiza productos PNG y GeoTIFF
    |
    |
    v
[Productos Finales]
  PNG + GeoTIFF



### Formatos y Fuentes Soportados

| Categoría | Detalles |
|---|---|
| **Formato de entrada** | BUFR (Forma Universal Binaria para Representación de datos meteorológicos) |
| **Formato intermedio** | NetCDF-4 / CF-Radial (vía arm-pyart) |
| **Formatos de salida** | Imágenes PNG, GeoTIFF optimizado para la nube (COG) |
| **Transporte de datos** | FTP asíncrono (vía `aioftp`) |
| **Redes de radares** | Red SiNaRaMe de Argentina (códigos RMA*) y cualquier radar compatible con BUFR |

### Características Principales

- [CHECK] **Pipeline completo de extremo a extremo** — descarga BUFR hasta entrega de productos en un servicio orquestado
- [CHECK] **Arquitectura asíncrona** — cada daemon se ejecuta como coroutine `asyncio` para I/O concurrente y no bloqueante
- [CHECK] **Gestión de estado tolerante a fallos** — `StateTracker` respaldado por SQLite permite reanudación automática tras caídas
- [CHECK] **Decodificación BUFR** — lector BUFR de alto rendimiento envolviendo bibliotecas compartidas Fortran/C
- [CHECK] **Integración PyART** — lee datos decodificados en objetos `pyart.core.Radar` con filtrado arbitrario de campos
- [CHECK] **Cálculo COLMAX** — reflectividad máxima columnar con límites de elevación configurables y filtros de calidad
- [CHECK] **Modos de salida duales** — imágenes PNG **o** GeoTIFFs optimizados en la nube con georreferencia EPSG:4326 correcta
- [CHECK] **Mapas de color personalizados** — 8+ mapas de color sintonizados por GRC para reflectividad, velocidad, ZDR, RhoHV, PhiDP, KDP
- [CHECK] **Configuración flexible** — cadena de override multinivel (variables de entorno → JSON → YAML → valores por defecto)
- [CHECK] **Listo para Docker** — despliegue de un contenedor por instancia de radar sin cambios de código
- [CHECK] **Pruebas completas** — 43+ archivos de prueba cubriendo escenarios unitarios e integración

### Arquitectura de Alto Nivel

| Módulo | Función |
|---|---|
| `radarlib.daemons` | Trabajadores asíncronos en segundo plano (descarga → procesamiento → producto → limpieza) |
| `radarlib.io.bufr` | Decodificación BUFR de bajo nivel mediante bibliotecas compartidas Fortran/C |
| `radarlib.io.ftp` | Cliente FTP asíncrono con reintentos y verificación de checksum |
| `radarlib.io.pyart` | Integración PyART: filtrado de campos, COLMAX, exportación PNG/GeoTIFF |
| `radarlib.radar_grid` | Motor de cuadrícula polar-cartesiana con geometría precomputada |
| `radarlib.state` | Seguimiento de estado SQLite y JSON para coordinación del pipeline |
| `radarlib.colormaps` | Mapas de color matplotlib personalizados para radar de polarización dual |
| `radarlib.config` | Configuración centralizada con cadena de override JSON/variables de entorno/YAML |
| `radarlib.utils` | Utilidades de nomenclatura de archivos, tipos de campo, utilidades de cuadrícula |

### Inicio Rápido (5 Minutos)

```python
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from radarlib.daemons import DaemonManager, DaemonManagerConfig

# Configura el pipeline para un radar
config = DaemonManagerConfig(
    radar_name="RMA1",
    base_path=Path("/data/radares/RMA1"),
    ftp_host="ftp.ejemplo.com",
    ftp_user="usuario_radar",
    ftp_password="contraseña_radar",
)

# Inicia el pipeline
async def main():
    manager = DaemonManager(config)
    await manager.start()

# Ejecútalo
asyncio.run(main())
```

---

# 2. Instalación & Configuración {#ch2-install}

## Requisitos del Sistema

### Versión de Python
- **Requerida:** Python >= 3.11, < 4.0

### Sistema Operativo
- **Principal:** Linux (Ubuntu 20.04+ recomendado)
- **Otros:** macOS, Windows (vía WSL2)

### Dependencias del Sistema (Linux)

Se requieren las bibliotecas nativas de GDAL:

```bash
sudo apt-get update
sudo apt-get install -y gdal-bin libgdal-dev build-essential git
```

### Requisitos de Hardware
- **CPU:** 2+ núcleos (para descompresión BUFR en paralelo)
- **RAM:** 4GB mínimo (8GB+ recomendado para datos de alta frecuencia)
- **Disco:** Depende de la política de retención; típicamente 100GB+ para datos activos de radar

## Métodos de Instalación

### 1. Desde Código Fuente (Desarrollo)

```bash
# Clonar el repositorio
git clone https://github.com/jgmarti84/gl-radarlib.git
cd gl-radarlib

# Crear y activar entorno virtual
python3.11 -m venv venv
source venv/bin/activate

# Instalar dependencias del sistema (Debian/Ubuntu)
sudo apt-get install -y gdal-bin libgdal-dev build-essential git

# Instalar el paquete en modo editable con dependencias
pip install -e .

# (Opcional) Instalar dependencias de desarrollo
pip install -r requirements-dev.txt
```

### 2. Usando el Makefile

```bash
# Crear entorno virtual
make venv

# Configurar: instalar dependencias + hooks pre-commit
make setup

# Ejecutar pruebas
make test

# Ejecutar linting (flake8)
make lint

# Formatear código (black)
make format
```

### 3. Despliegue Docker

```bash
# Construir la imagen Docker
docker build -t radarlib:latest .

# Ejecutar un contenedor para un radar específico
docker run -d \
  -e FTP_HOST="ftp.ejemplo.com" \
  -e FTP_USER="usuario" \
  -e FTP_PASS="contraseña" \
  -v /data/radares:/data/radares \
  -v /output/productos:/output/productos \
  radarlib:latest
```

## Credenciales & Configuración de Entorno

### Opción 1: Variables de Entorno

```bash
export FTP_HOST="ftp.tu-servidor-radar.com"
export FTP_USER="tu_usuario"
export FTP_PASS="tu_contraseña"
export BUFR_RESOURCES_PATH="/ruta/a/recursos/bufr"
export ROOT_RADAR_PRODUCTS_PATH="/ruta/a/salida/productos"
```

### Opción 2: Archivo de Configuración

Ver [Referencias de Configuración](#3-referencias-de-configuración) para detalles sobre uso de `genpro25.yml`.

---

# 3. Referencias de Configuración {#ch3-config}

## Sistema de Configuración

radarlib utiliza un sistema de configuración flexible con múltiples niveles:

1. **Variables de entorno** (prioridad máxima)
2. **Archivo de configuración JSON** (vía env var `RADARLIB_CONFIG`)
3. **Archivo YAML** (`genpro25.yml`, si está presente)
4. **Valores por defecto integrados** (prioridad mínima)

## Carga de Configuración

```python
from radarlib import config

# Obtener un valor de configuración
valor = config.get("CLAVE")

# Obtener con valor por defecto
valor = config.get("CLAVE", default="fallback")

# Recargar desde archivo
config.reload("/ruta/a/config.json")
```

## Variables de Entorno Clave

### Rutas Generales

| Variable | Descripción | Tipo | Por Defecto |
|---|---|---|---|
| `RADARLIB_CONFIG` | Ruta al archivo JSON de configuración | `str` | - |
| `BUFR_RESOURCES_PATH` | Ruta a tablas BUFR y biblioteca C | `str` | `<paquete>/io/bufr/bufr_resources` |
| `ROOT_CACHE_PATH` | Directorio de caché | `str` | `<proyecto>/cache` |
| `ROOT_RADAR_FILES_PATH` | Directorio base de archivos de radar | `str` | `<proyecto>/data/radares` |
| `ROOT_RADAR_PRODUCTS_PATH` | Directorio de salida de productos | `str` | `<proyecto>/product_output` |
| `ROOT_GATE_COORDS_PATH` | Caché de coordenadas de gates | `str` | `<proyecto>/data/gate_coordinates` |
| `ROOT_GEOMETRY_PATH` | Caché de geometrías | `str` | `<proyecto>/data/geometries` |
| `ROOT_LOGS_PATH` | Directorio de logs | `str` | `<proyecto>/logs` |

### Configuración FTP

| Variable | Descripción | Tipo | Por Defecto |
|---|---|---|---|
| `FTP_HOST` | Nombre de host del servidor FTP | `str` | `"www.ejemplo.com"` |
| `FTP_USER` | Usuario FTP | `str` | `"ejemplo_usuario"` |
| `FTP_PASS` | Contraseña FTP | `str` | `"secreto"` |

### Procesamiento COLMAX (Máximo Columnar)

| Variable | Descripción | Tipo | Por Defecto |
|---|---|---|---|
| `COLMAX_THRESHOLD` | Umbral de reflectividad (dBZ) | `float` | `-3` |
| `COLMAX_ELEV_LIMIT1` | Ángulo de elevación máximo | `float` | `0.65` |
| `COLMAX_RHOHV_FILTER` | Habilitar filtro RhoHV | `bool` | `True` |
| `COLMAX_RHOHV_UMBRAL` | Umbral de calidad RhoHV | `float` | `0.8` |
| `COLMAX_WRAD_FILTER` | Habilitar filtro de ancho espectral | `bool` | `True` |
| `COLMAX_WRAD_UMBRAL` | Umbral de ancho espectral | `float` | `4.6` |
| `COLMAX_TDR_FILTER` | Habilitar filtro ZDR | `bool` | `True` |
| `COLMAX_TDR_UMBRAL` | Umbral ZDR | `float` | `8.5` |

### Visualización (Datos Sin Filtrar)

| Variable | Descripción | Tipo | Por Defecto |
|---|---|---|---|
| `VMIN_REFL_NOFILTERS` | Mínimo reflectividad | `int` | `-20` |
| `VMAX_REFL_NOFILTERS` | Máximo reflectividad | `int` | `70` |
| `CMAP_REFL_NOFILTERS` | Mapa de color reflectividad | `str` | `"grc_th"` |
| `VMIN_RHOHV_NOFILTERS` | Mínimo RhoHV | `int` | `0` |
| `VMAX_RHOHV_NOFILTERS` | Máximo RhoHV | `int` | `1` |
| `CMAP_RHOHV_NOFILTERS` | Mapa de color RhoHV | `str` | `"grc_rho"` |
| `VMIN_ZDR_NOFILTERS` | Mínimo ZDR | `float` | `-7.5` |
| `VMAX_ZDR_NOFILTERS` | Máximo ZDR | `float` | `7.5` |
| `CMAP_ZDR_NOFILTERS` | Mapa de color ZDR | `str` | `"grc_zdr"` |
| `VMIN_VRAD_NOFILTERS` | Mínimo velocidad radial | `int` | `-30` |
| `VMAX_VRAD_NOFILTERS` | Máximo velocidad radial | `int` | `30` |
| `CMAP_VRAD_NOFILTERS` | Mapa de color velocidad radial | `str` | `"grc_vrad"` |

### Visualización (Datos Filtrados)

| Variable | Descripción | Tipo | Por Defecto |
|---|---|---|---|
| `VMIN_REFL` | Mínimo reflectividad (filtrado) | `int` | `-20` |
| `VMAX_REFL` | Máximo reflectividad (filtrado) | `int` | `70` |
| `CMAP_REFL` | Mapa de color reflectividad (filtrado) | `str` | `"grc_th"` |
| `VMIN_RHOHV` | Mínimo RhoHV (filtrado) | `int` | `0` |
| `VMAX_RHOHV` | Máximo RhoHV (filtrado) | `int` | `1` |
| `CMAP_RHOHV` | Mapa de color RhoHV (filtrado) | `str` | `"grc_rho"` |
| `VMIN_ZDR` | Mínimo ZDR (filtrado) | `float` | `-2.0` |
| `VMAX_ZDR` | Máximo ZDR (filtrado) | `float` | `7.5` |
| `CMAP_ZDR` | Mapa de color ZDR (filtrado) | `str` | `"grc_zdr"` |
| `VMIN_VRAD` | Mínimo velocidad radial (filtrado) | `int` | `-15` |
| `VMAX_VRAD` | Máximo velocidad radial (filtrado) | `int` | `15` |
| `CMAP_VRAD` | Mapa de color velocidad (filtrado) | `str` | `"grc_vrad"` |

## Ejemplo: genpro25.yml

```yaml
local:
  COLMAX:
    COLMAX_THRESHOLD: -3
    COLMAX_ELEV_LIMIT1: 0.65
    COLMAX_RHOHV_FILTER: true
    COLMAX_RHOHV_UMBRAL: 0.8
    COLMAX_WRAD_FILTER: true
    COLMAX_WRAD_UMBRAL: 4.6
  FTP:
    FTP_HOST: "200.16.116.24"
    FTP_USER: "tu_usuario"
    FTP_PASS: "tu_contraseña"
  PNG_PLOTS:
    FIELDS_TO_PLOT: ["DBZH", "RHOHV", "ZDR"]
    VMIN_REFL: -20
    VMAX_REFL: 70
    CMAP_REFL: "grc_th"
  GRC_FILTER:
    RHV:
      GRC_RHV_FILTER: true
      GRC_RHV_THRESHOLD: 0.8
```

---

# 4. Despliegue & Operaciones {#ch4-deployment}

## Despliegue Docker

### 1. Despliegue de Radar Único

```bash
# Construir imagen
docker build -t radarlib:latest .

# Ejecutar contenedor
docker run -d \
  --name radarlib-rma1 \
  -e FTP_HOST="ftp.ejemplo.com" \
  -e FTP_USER="usuario" \
  -e FTP_PASS="contraseña" \
  -v /data/radares/RMA1:/data/radares/RMA1 \
  -v /output/productos:/output/productos \
  radarlib:latest
```

### 2. Despliegue Multi-Radar (Docker Compose)

```yaml
version: '3.8'
services:
  rma1:
    build: .
    environment:
      - FTP_HOST=ftp.ejemplo.com
      - FTP_USER=usuario1
      - FTP_PASS=contraseña1
      - RADAR_NAME=RMA1
    volumes:
      - /data/radares/RMA1:/data/radares/RMA1
      - /output/productos:/output/productos
    restart: unless-stopped

  rma11:
    build: .
    environment:
      - FTP_HOST=ftp.ejemplo.com
      - FTP_USER=usuario2
      - FTP_PASS=contraseña2
      - RADAR_NAME=RMA11
    volumes:
      - /data/radares/RMA11:/data/radares/RMA11
      - /output/productos:/output/productos
    restart: unless-stopped
```

### 3. Monitorear Logs

```bash
# Logs de Docker para contenedor específico
docker logs -f radarlib-rma1

# Verificar estado
docker ps | grep radarlib

# Reiniciar instancia
docker restart radarlib-rma1
```

## Resolución Rápida de Problemas

### Problema: Falla Conexión FTP
- **Verificar:** Las credenciales FTP son correctas en variables de entorno o config
- **Verificar:** El servidor FTP es accesible: `ftp-ping ftp.ejemplo.com`
- **Solución:** Verificar `FTP_HOST`, `FTP_USER`, `FTP_PASS`

### Problema: No se Generan Archivos de Salida
- **Verificar:** Los archivos BUFR se descargan: `ls /data/radares/RADAR_NAME/`
- **Verificar:** Logs del daemon de procesamiento: `docker logs radarlib-rma1`
- **Verificar:** El directorio de salida existe y tiene permisos de escritura

### Problema: Sin Espacio en Disco
- **Solución:** Configurar política de retención del daemon de limpieza
- **Verificar:** `du -sh /output/productos` para ver tamaño actual
- **Solución:** Archivar productos antiguos a almacenamiento externo

### Problema: Uso Alto de Memoria
- **Causa:** Posiblemente descompresión BUFR en paralelo
- **Solución:** Desactivar modo paralelo o reducir paralelismo en configuración

---

# 5. Arquitectura Profunda {#ch5-architecture}

## Arquitectura del Sistema

### Daemon Manager & Daemons

radarlib utiliza una arquitectura basada en daemons asíncronos donde trabajadores independientes procesan diferentes etapas del pipeline:

```

                   GESTOR DE DAEMONS
  Orquesta todos los daemons para pipeline completo



    DAEMON DE        DAEMON DE
    DESCARGA             PROCESAMIENTO

   • Monitor FTP        • Decodificar
   • Descargar            BUFR
   • Verificar          • Convertir
     checksum             PyART
       • Guardar
                           NetCDF


      DAEMON GENERACIÓN DE PRODUCTOS

   • Leer NetCDF
   • Generar COLMAX
   • Renderizar PNG
   • Guardar GeoTIFF (COG)




     DAEMON DE LIMPIEZA

   • Eliminar productos antiguos
   • Aplicar política de retención



     Base de Datos SQLite

   • Tabla downloads
   • Tabla volumes
   • Tabla product_generation


```

### Flujo de Datos

1. **Etapa de Descarga**
   - `DownloadDaemon` monitorea servidor FTP cada N segundos
   - Descarga nuevos archivos BUFR a `ROOT_RADAR_FILES_PATH/<radar_name>/`
   - Verifica checksums
   - Registra estado en SQLite

2. **Etapa de Procesamiento**
   - `ProcessingDaemon` consulta archivos BUFR descargados
   - Utiliza `radarlib.io.bufr.bufr_to_dict()` para decodificar
   - Convierte a objeto PyART `Radar`
   - Guarda como NetCDF CF-Radial a `ROOT_RADAR_FILES_PATH/<radar_name>/`
   - Registra estado (seguro ante fallos, reanudable)

3. **Etapa de Generación de Productos**
   - `ProductGenerationDaemon` consulta archivos NetCDF procesados
   - Genera COLMAX (reflectividad máxima columnar)
   - Crea visualizaciones PPI (Plan Position Indicator)
   - Exporta imágenes PNG a `ROOT_RADAR_PRODUCTS_PATH/<radar_name>/`
   - Exporta GeoTIFF (COG) al mismo directorio
   - Registra estado

4. **Etapa de Limpieza**
   - `CleanupDaemon` aplica políticas de retención
   - Elimina archivos más antiguos que período configurado
   - Gestiona espacio en disco

## Gestión de Estado

Todos los daemons utilizan **seguimiento de estado respaldado por SQLite** para asegurar:
- [CHECK] Los archivos nunca se procesan dos veces
- [CHECK] Reanudación desde punto exacto después de fallo
- [CHECK] Las transacciones atómicas previenen escrituras de estado parciales

```python
# Ejemplo: uso de StateTracker en un daemon
from radarlib.state import StateTracker

tracker = StateTracker(db_path="/path/to/state.db")
tracker.record_download(filename, file_hash, timestamp)
tracker.record_volume(filename, volume_hash)
tracker.record_product(volume_id, product_type, output_path)
```

---

# 6. Referencia de Módulos {#ch6-modules}

## Módulos Principales

### radarlib.config

Gestión centralizada de configuración con cadena de override multinivel.

**Funciones Clave:**
- `config.get(key: str, default: Any = None) -> Any` — Obtener valor de configuración
- `config.reload(path: str | Path) -> None` — Recargar desde archivo
- `config.to_dict() -> dict` — Exportar toda configuración actual

**Ejemplo:**
```python
from radarlib import config

# Obtener rutas configurables
ruta_productos = config.get("ROOT_RADAR_PRODUCTS_PATH")
host_ftp = config.get("FTP_HOST")
```

### radarlib.io.bufr

Decodificación y análisis de archivos BUFR. **Envoltorio Python alrededor de bibliotecas C/Fortran.**

**Funciones Clave:**
- `bufr_to_dict(filename: str) -> dict | None` — Interfaz de decodificación de alto nivel
- `dec_bufr_file(filename: str, ...)` — Decodificación de bajo nivel con control total
- `bufr_name_metadata(filename: str) -> dict` — Analizar nombre de archivo BUFR

**Ejemplo:**
```python
from radarlib.io.bufr import bufr_to_dict

result = bufr_to_dict("AR5_1000_1_DBZH_20240101T000746Z.BUFR")
if result:
    data = result['data']  # ndarray numpy (rays, gates)
    info = result['info']  # diccionario de metadatos
```

### radarlib.io.ftp

Cliente FTP asíncrono para descarga confiable de datos.

**Clases Clave:**
- `FTPClient` — Conexión FTP asíncrona con lógica de reintentos
- `FileAvailabilityChecker` — Verificar existencia de archivo en FTP
- `FTPDownloader` — Descarga de alto nivel con checksums

**Ejemplo:**
```python
import asyncio
from radarlib.io.ftp import FTPDownloader

async def descargar_datos_radar():
    downloader = FTPDownloader(host="ftp.ejemplo.com", user="usuario", password="contraseña")
    await downloader.download("ruta/remota/archivo.BUFR", "ruta/local/archivo.BUFR")

asyncio.run(descargar_datos_radar())
```

### radarlib.io.pyart

Integración PyART para manipulación de objetos Radar y generación de productos.

**Funciones Clave:**
- `radar_to_pyart(decoded_bufr: dict) -> pyart.core.Radar` — Convertir BUFR decodificado a Radar
- `apply_grc_filters(radar: Radar, ...) -> Radar` — Aplicar filtros de calidad GRC
- `export_to_geotiff(radar: Radar, field: str, output_path: str, ...) -> None` — Guardar como GeoTIFF
- `export_to_png(radar: Radar, field: str, output_path: str, ...) -> None` — Guardar como PNG

### radarlib.radar_grid

Motor de cuadrícula polar-cartesiana precomputada para interpolación eficiente.

**Funciones Clave:**
- `CartesianGrid(radar: Radar, **kwargs)` — Crear cuadrícula de interpolación
- `grid.get_cart_grid(field: str) -> ndarray` — Interpolar campo a cuadrícula

### radarlib.daemons

Orquestación principal del pipeline (descarga, procesamiento, producto, limpieza).

**Clases Clave:**

#### DownloadDaemon
```python
class DownloadDaemon:
    """Monitorea FTP y descarga nuevos archivos BUFR."""
    async def start()  # Iniciar bucle de monitoreo
    async def stop()   # Cierre ordenado
```

#### ProcessingDaemon
```python
class ProcessingDaemon:
    """Decodifica BUFR → volúmenes NetCDF."""
    async def start()
    async def stop()
```

#### ProductGenerationDaemon
```python
class ProductGenerationDaemon:
    """Genera PNG/GeoTIFF desde NetCDF."""
    async def start()
    async def stop()
```

#### DaemonManager
```python
class DaemonManager:
    """Orquesta todos los daemons para pipeline completo."""
    async def start()  # Iniciar todos los daemons
    async def stop()   # Detener todos los daemons
```

---

# 7. Guía de Procesamiento BUFR {#ch7-bufr}

## ¿Qué es BUFR?

BUFR (Forma Universal Binaria para Representación de datos meteorológicos) es un formato estandarizado de la Organización Meteorológica Mundial (OMM) para codificar datos meteorológicos. Se usa ampliamente por servicios meteorológicos nacionales para archivar y transmitir observaciones de radar.

## Estructura de Archivos BUFR

Un archivo BUFR de radar contiene:
- **Encabezado del mensaje** — marca de tiempo, ubicación del radar, metadatos del sitio
- **Ángulos fijos** — ángulos de elevación de cada barrido
- **Datos del volumen** — campos de reflectividad codificados (comprimidos)

## Pipeline de Decodificación

El pipeline de decodificación BUFR de radarlib:

```
1. Cargar biblioteca C (libdecbufr.so)
   ↓
2. Leer tamaño de volumen, elevaciones, buffer entero sin procesar
   ↓
3. Analizar buffer entero en encabezados por barrido + trozos de datos comprimidos
   ↓
4. Descomprimir datos por barrido (zlib)
   ↓
5. Convertir a arrays 2-D (rays × gates por barrido)
   ↓
6. Uniformizar conteos de gates en barridos (rellenar con NaN)
   ↓
7. Concatenar verticalmente en array de volumen único
   ↓
8. Construir diccionario de metadatos
```

## Uso de Alto Nivel

```python
from radarlib.io.bufr import bufr_to_dict

# Decodificar archivo BUFR
result = bufr_to_dict("AR5_1000_1_DBZH_20240101T000746Z.BUFR")

if result:
    # Acceder a datos decodificados
    volume_data = result['data']        # ndarray (total_rays, gates)
    metadata = result['info']           # dict con info de barridos, ubicación radar, etc.

    # Ejemplo: convertir a Radar PyART
    from radarlib.io.pyart import radar_to_pyart
    radar = radar_to_pyart(result)
    print(f"Ubicación radar: {radar.latitude['data'][0]}, {radar.longitude['data'][0]}")
else:
    print("Fallo en decodificación BUFR")
```

## API de Bajo Nivel

Para control más fino sobre decodificación:

```python
from radarlib.io.bufr.bufr import (
    dec_bufr_file, parse_sweeps, decompress_sweep, uniformize_sweeps
)

# Decodificación completa con control
meta_vol, sweeps, vol_data, run_log = dec_bufr_file(
    bufr_filename="archivo.BUFR",
    parallel=True,  # Usar ThreadPoolExecutor para descompresión
    logger_name="logger_personalizado"
)

# Acceder a datos por barrido
for i, sweep in enumerate(sweeps):
    print(f"Barrido {i}: {sweep['data'].shape} (rays, gates)")
```

## Manejo de Errores

Excepciones comunes:

```python
from radarlib.io.bufr.bufr import SweepConsistencyException

try:
    result = bufr_to_dict("archivo.BUFR")
except SweepConsistencyException as e:
    # Barrido malo fue saltado
    print(f"Barrido saltado por inconsistencia: {e}")
except ValueError as e:
    # Error de descompresión o formato de datos
    print(f"Error durante decodificación: {e}")
```

## Plantillas BUFR Soportadas

radarlib soporta plantillas BUFR comúnmente usadas por la red SiNaRaMe de Argentina:
- **Plantilla 0315** — Volúmenes de radar multi-barrido
  - Subconjunto 01: Campos de reflectividad (DBZH, DBZV, ZDR, RHOHV, PHIDP, KDP)
  - Subconjunto 02: Campos de velocidad (VRAD, WRAD)

---

# 8. Integración & Ejemplos Avanzados {#ch8-integration}

## Usar radarlib como Biblioteca

### Ejemplo 1: Procesar un Archivo BUFR Único

```python
from pathlib import Path
from radarlib.io.bufr import bufr_to_dict
from radarlib.io.pyart import export_to_geotiff, export_to_png

# Decodificar BUFR
result = bufr_to_dict("datos_radar.BUFR")
if not result:
    raise ValueError("Fallo en decodificación BUFR")

# Convertir a PyART
from radarlib.io.pyart import radar_to_pyart
radar = radar_to_pyart(result)

# Exportar como PNG y GeoTIFF
export_to_png(
    radar=radar,
    field="DBZH",
    output_path="potencia_reflejada.png",
    vmin=-20, vmax=70, colormap="grc_th"
)

export_to_geotiff(
    radar=radar,
    field="DBZH",
    output_path="potencia_reflejada.tif",
    vmin=-20, vmax=70, colormap="grc_th"
)
```

### Ejemplo 2: Procesamiento Multi-Radar

```python
import asyncio
from pathlib import Path
from radarlib.daemons import DaemonManager, DaemonManagerConfig

async def procesar_multiples_radares():
    radares = ["RMA1", "RMA11", "RMA5"]
    managers = []

    for nombre_radar in radares:
        config = DaemonManagerConfig(
            radar_name=nombre_radar,
            base_path=Path(f"/data/radares/{nombre_radar}"),
            ftp_host="ftp.ejemplo.com",
            ftp_user="usuario",
            ftp_password="contraseña",
        )
        manager = DaemonManager(config)
        managers.append(manager)

    # Iniciar todos los pipelines concurrentemente
    await asyncio.gather(*[m.start() for m in managers])

asyncio.run(procesar_multiples_radares())
```

### Ejemplo 3: Pipeline de Procesamiento de Campo Personalizado

```python
from radarlib.io.pyart import apply_grc_filters, export_to_geotiff
from radarlib.io.bufr import bufr_to_dict
from radarlib.io.pyart import radar_to_pyart

# Decodificar y convertir
result = bufr_to_dict("datos_radar.BUFR")
radar = radar_to_pyart(result)

# Aplicar filtros de control de calidad (metodología GRC)
radar_filtrado = apply_grc_filters(
    radar=radar,
    rhohv_filter=True,
    rhohv_threshold=0.8,
    zdr_filter=True,
    zdr_threshold=8.5,
    wrad_filter=True,
    wrad_threshold=4.6
)

# Computar COLMAX (reflectividad máxima columnar)
from radarlib.io.pyart import compute_colmax
colmax = compute_colmax(
    radar=radar_filtrado,
    field="DBZH",
    elev_limit=0.65,
    threshold=-3
)

# Exportar resultado
export_to_geotiff(
    radar=colmax,
    field="COLMAX",
    output_path="producto_colmax.tif",
    vmin=-3, vmax=70, colormap="grc_th"
)
```

## Contrato de Salida (CRÍTICO)

> [ADVERTENCIA] **Esta sección es crítica.** El repositorio `webmet25` consume los archivos de salida producidos por radarlib. Nunca cambies este contrato sin actualizar webmet25 también.

### Tipos de Archivo

- **GeoTIFF (.tif)** — GeoTIFF optimizado para la nube con georreferencia y metadatos
- **PNG (.png)** — Visualización PNG georeferenciada

### Convención de Nomenclatura de Archivos

```
<CAMPO>_<TIMESTAMP>.<ext>
```

**Ejemplos:**
- `DBZH_20250101T120000Z.tif` (GeoTIFF Reflectividad)
- `DBZH_20250101T120000Z.png` (PNG Reflectividad)
- `COLMAX_20250101T120000Z.tif` (GeoTIFF Máximo Columnar)
- `RHOHV_20250101T120000Z.tif` (GeoTIFF Correlación Copolar)

### Estructura de Carpetas

```
ROOT_RADAR_PRODUCTS_PATH/
 <RADAR_NAME>/
     DBZH_20250101T120000Z.tif
     DBZH_20250101T120000Z.png
     COLMAX_20250101T120000Z.tif
     COLMAX_20250101T120000Z.png
     RHOHV_20250101T120000Z.tif
     RHOHV_20250101T120000Z.png
     ...
```

### Campos de Metadatos de GeoTIFF

| Campo | Valor | Propósito |
|---|---|---|
| **CRS** | EPSG:4326 | Sistema de coordenadas geográficas (WGS84 lat/lon) |
| **radarlib_cmap** | Nombre de mapa de color string | Nombre del mapa de color matplotlib usado (ej. `"grc_th"`) |
| **vmin** | Float | Valor mínimo de datos para escala de color |
| **vmax** | Float | Valor máximo de datos para escala de color |
| **field_name** | String | Nombre de campo de radar (ej. `"DBZH"`) |
| **timestamp** | ISO 8601 | Marca de tiempo de adquisición de datos |

### Propiedades de GeoTIFF

- **Formato:** GeoTIFF optimizado para la nube (COG)
- **Compresión:** Deflate o LZW
- **Tamaño de Bloque:** 512×512 (optimizado para tiling)
- **Overviews:** Generados para acceso multi-escala
- **Tipo de Datos:** Float32

### Propiedades de PNG

- **Formato:** PNG RGBA (con canal alfa para georreferencia)
- **DPI:** 100-150 (configurable)
- **Mapa de Color:** Aplicado usando matplotlib
- **Georreferencia:** Incrustada en metadatos (opcional, para soporte software GIS)

---

## Brechas Conocidas & Riesgos

### Manejo de Errores
- [INCOMPLETE] Manejo de errores limitado en daemons (fallos FTP, corrupción de archivos)
- [INCOMPLETE] Sin lógica de reintentos para pasos de procesamiento fallidos
- [FIX] **Recomendación:** Implementar backoff exponencial + cola de letras muertas

### Pruebas
- [INCOMPLETE] Cobertura de pruebas incompleta para módulo `radar_grid`
- [INCOMPLETE] Sin pruebas de integración para pipeline completo end-to-end
- [INCOMPLETE] Sin pruebas para validación de salida GeoTIFF (CRS, completitud de metadatos)
- [FIX] **Recomendación:** Añadir fixtures de pytest para datos realistas

### Validación de Salida
- [INCOMPLETE] Sin validación que GeoTIFF tiene CRS correcto
- [INCOMPLETE] Sin validación que campos de metadatos están presentes
- [FIX] **Recomendación:** Implementar función `validate_geotiff()` en etapa de salida

### Escalabilidad
- [INCOMPLETE] El seguimiento de estado SQLite puede convertirse en cuello de botella con datos de alta frecuencia (< 1 min intervalos)
- [FIX] **Recomendación:** Considerar PostgreSQL para sistemas de producción

### Documentación
- [INCOMPLETE] Opciones de configuración en `genpro25.yml` están pobremente documentadas
- [INCOMPLETE] Soporte de plantillas BUFR y limitaciones no son claros
- [FIX] **Recomendación:** Mantener documentación de esquema autoritativa

### Despliegue
- [INCOMPLETE] Sin configuraciones específicas de producción (ej. `docker-compose.prod.yml`)
- [INCOMPLETE] Sin manifiestos de Kubernetes
- [FIX] **Recomendación:** Crear carpeta `deploy/k8s/` con gráficos Helm

---

## Contribuyendo

¡Las contribuciones son bienvenidas! Por favor sigue estas directrices:

1. **Estilo de código:** black + flake8
2. **Anotaciones de tipos:** Requeridas en todas las funciones (aplicadas por mypy)
3. **Pruebas:** Añade pruebas para nueva funcionalidad
4. **Documentación:** Actualiza secciones de docs/README

## Licencia

Este proyecto está licenciado bajo la Licencia MIT. Ver el archivo [LICENSE](../LICENSE) para detalles.

## Reconocimientos

Desarrollado por **Grupo Radar Córdoba (GRC)** — Universidad Nacional de Córdoba, Argentina.

---

**Última Actualización:** 2 de abril de 2026
**Versión:** 0.1.0
