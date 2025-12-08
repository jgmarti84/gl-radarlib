# 5. Módulos Principales

## Cliente FTP

### Descripción

El módulo FTP proporciona clientes síncronos y asíncronos para conectarse a servidores FTP y descargar archivos BUFR de radar.

### Clases Principales

#### `RadarFTPClient`

Cliente FTP síncrono para operaciones básicas.

```python
from radarlib.io.ftp import RadarFTPClient

# Uso como context manager (recomendado)
with RadarFTPClient(
    host="ftp.servidor.com",
    user="usuario",
    password="contraseña",
    base_dir="L2",
    timeout=30
) as client:

    # Listar directorio
    archivos = client.list_dir("/L2/RMA1/2025/01/01/12")

    # Descargar archivo
    from pathlib import Path
    client.download_file(
        "/L2/RMA1/archivo.BUFR",
        Path("./local/archivo.BUFR")
    )

    # Recorrer estructura de radar
    from datetime import datetime, timezone

    for dt, filename, remote_path in client.traverse_radar(
        radar_name="RMA1",
        dt_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        dt_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
    ):
        print(f"{dt}: {filename}")
```

#### `RadarFTPClientAsync`

Cliente FTP asíncrono para descargas paralelas.

```python
import asyncio
from radarlib.io.ftp import RadarFTPClientAsync
from pathlib import Path

async def descargar_archivos():
    async with RadarFTPClientAsync(
        host="ftp.servidor.com",
        user="usuario",
        password="contraseña",
        max_workers=5
    ) as client:

        # Descargar archivo individual
        await client.download_file_async(
            Path("/L2/RMA1/archivo.BUFR"),
            Path("./local/archivo.BUFR")
        )

        # Descargar múltiples archivos en paralelo
        archivos = [
            (Path("/L2/RMA1/archivo1.BUFR"), Path("./local/archivo1.BUFR")),
            (Path("/L2/RMA1/archivo2.BUFR"), Path("./local/archivo2.BUFR")),
        ]
        resultados = await client.download_files_parallel(archivos)

asyncio.run(descargar_archivos())
```

### Funciones Utilitarias

```python
from radarlib.io.ftp import (
    ftp_connection_manager,
    list_files_in_remote_dir,
    download_file_from_ftp,
    download_multiple_files_from_ftp,
    exponential_backoff_retry
)

# Context manager para conexión FTP
with ftp_connection_manager(host, user, password) as ftp:
    archivos = list_files_in_remote_dir(ftp, "/L2/RMA1")

# Descarga con reintentos automáticos
await exponential_backoff_retry(
    lambda: client.download_file_async(remote, local),
    max_retries=3,
    base_delay=1.0,
    max_delay=30.0
)
```

---

## Procesamiento BUFR

### Descripción

El módulo BUFR proporciona funcionalidades para decodificar archivos BUFR de radar utilizando una biblioteca C optimizada.

### Función Principal: `bufr_to_dict`

```python
from radarlib.io.bufr.bufr import bufr_to_dict

# Decodificar archivo BUFR
resultado = bufr_to_dict(
    bufr_filename="RMA1_0315_01_DBZH_20250101T120000Z.BUFR",
    root_resources=None,  # Usa config.BUFR_RESOURCES_PATH
    legacy=False          # Formato moderno con DataFrame
)

if resultado is not None:
    # Datos del volumen (numpy array 2D)
    data = resultado['data']
    print(f"Shape: {data.shape}")  # (total_rays, ngates)

    # Información del volumen
    info = resultado['info']
    print(f"Radar: {info['nombre_radar']}")
    print(f"Estrategia: {info['estrategia']['nombre']}")
    print(f"Tipo producto: {info['tipo_producto']}")
    print(f"Fecha: {info['ano_vol']}/{info['mes_vol']}/{info['dia_vol']}")
    print(f"Hora: {info['hora_vol']}:{info['min_vol']}")
    print(f"Ubicación: {info['lat']}, {info['lon']}")
    print(f"Altitud: {info['altura']} m")

    # Información de barridos (DataFrame)
    sweeps_df = info['sweeps']
    print(f"Número de barridos: {info['nsweeps']}")
    print(f"Elevaciones: {sweeps_df['elevaciones'].tolist()}")
```

### Estructura del Diccionario Resultante

```python
{
    'data': np.ndarray,  # Shape: (total_rays, ngates), dtype: float64
    'info': {
        'nombre_radar': str,         # Ej: "RMA1"
        'estrategia': {
            'nombre': str,           # Ej: "0315"
            'volume_number': str     # Ej: "01"
        },
        'tipo_producto': str,        # Ej: "DBZH", "VRAD", etc.
        'filename': str,             # Nombre del archivo original
        'ano_vol': int,              # Año
        'mes_vol': int,              # Mes
        'dia_vol': int,              # Día
        'hora_vol': int,             # Hora
        'min_vol': int,              # Minuto
        'lat': float,                # Latitud del radar
        'lon': float,                # Longitud del radar
        'altura': float,             # Altitud del radar (m)
        'nsweeps': int,              # Número de barridos
        'sweeps': pd.DataFrame,      # Info de cada barrido
        'metadata': dict             # Metadatos para NetCDF
    }
}
```

### Funciones de Bajo Nivel

```python
from radarlib.io.bufr.bufr import (
    decbufr_library_context,
    get_metadata,
    get_elevations,
    get_raw_volume,
    parse_sweeps,
    decompress_sweep,
    uniformize_sweeps,
    assemble_volume
)

# Usar biblioteca C directamente
with decbufr_library_context() as lib:
    # Obtener metadatos
    meta = get_metadata(lib, "archivo.BUFR")
    print(f"Ubicación: {meta['lat']}, {meta['lon']}")

    # Obtener elevaciones
    elevs = get_elevations(lib, "archivo.BUFR")
    print(f"Elevaciones: {elevs}")
```

---

## Integración PyART

### Descripción

El módulo proporciona conversión de datos BUFR a objetos Radar de PyART y guardado en formato NetCDF CFRadial.

### Conversión BUFR a PyART

```python
from radarlib.io.bufr.bufr import bufr_to_dict
from radarlib.io.bufr.bufr_to_pyart import (
    bufr_fields_to_pyart_radar,
    bufr_paths_to_pyart,
    bufr_to_pyart,
    save_radar_to_cfradial
)

# Método 1: Desde diccionarios decodificados
vol_dbzh = bufr_to_dict("RMA1_0315_01_DBZH_20250101T120000Z.BUFR")
vol_vrad = bufr_to_dict("RMA1_0315_02_VRAD_20250101T120000Z.BUFR")

radar = bufr_fields_to_pyart_radar([vol_dbzh, vol_vrad])

# Método 2: Directamente desde rutas de archivos
radar = bufr_paths_to_pyart(
    [
        "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",
        "RMA1_0315_02_VRAD_20250101T120000Z.BUFR"
    ],
    save_path=Path("./output_netcdf")  # Opcional: guardar NetCDF
)

# Guardar radar a NetCDF
from pathlib import Path
save_radar_to_cfradial(radar, Path("./radar_output.nc"))
```

### Objeto Radar de PyART

```python
# Propiedades del objeto Radar
print(f"Número de rayos: {radar.nrays}")
print(f"Número de gates: {radar.ngates}")
print(f"Número de barridos: {radar.nsweeps}")
print(f"Campos disponibles: {list(radar.fields.keys())}")

# Acceder a datos de campo
reflectividad = radar.fields['DBZH']['data']
velocidad = radar.fields['VRAD']['data']

# Información geográfica
print(f"Latitud: {radar.latitude['data'][0]}")
print(f"Longitud: {radar.longitude['data'][0]}")
print(f"Altitud: {radar.altitude['data'][0]} m")

# Información de barridos
print(f"Elevaciones fijas: {radar.fixed_angle['data']}")
```

---

## Visualización

### Generación de Imágenes PNG

```python
from radarlib.io.pyart.radar_png_plotter import (
    RadarPlotConfig,
    FieldPlotConfig,
    plot_ppi_field,
    save_ppi_png,
    plot_and_save_ppi,
    plot_multiple_fields
)

# Configuración de gráfico
plot_config = RadarPlotConfig(
    figsize=(15, 15),
    dpi=150,
    transparent=True,
    colorbar=False,
    title=False,
    axis_labels=False
)

# Configuración de campo con valores personalizados
field_config = FieldPlotConfig(
    field_name="DBZH",
    vmin=-20,
    vmax=70,
    cmap="grc_th",
    sweep=0
)

# Generar gráfico PPI
fig, ax = plot_ppi_field(
    radar,
    field="DBZH",
    sweep=0,
    config=plot_config,
    field_config=field_config
)

# Guardar como PNG
save_ppi_png(
    fig,
    output_path="./products",
    filename="DBZH_sweep00.png",
    dpi=150,
    transparent=True
)

# O en una sola llamada
output_path = plot_and_save_ppi(
    radar,
    field="DBZH",
    output_path="./products",
    filename="DBZH_sweep00.png",
    sweep=0,
    config=plot_config,
    field_config=field_config
)

# Generar múltiples campos
resultados = plot_multiple_fields(
    radar,
    fields=["DBZH", "VRAD", "ZDR", "RHOHV"],
    output_base_path="./products",
    sweep=0,
    config=plot_config
)
```

### Exportación GeoTIFF

```python
from radarlib.io.pyart.radar_geotiff_exporter import (
    save_ppi_field_to_geotiff,
    save_multiple_fields_to_geotiff,
    radar_to_netcdf_with_coordinates
)

# Exportar campo individual
ruta = save_ppi_field_to_geotiff(
    radar,
    field="DBZH",
    output_path="./geotiff",
    filename="DBZH_sweep00.tif",
    sweep=0,
    crs="EPSG:4326"
)

# Exportar múltiples campos
resultados = save_multiple_fields_to_geotiff(
    radar,
    fields=["DBZH", "VRAD", "ZDR"],
    output_base_path="./geotiff",
    sweep=0,
    crs="EPSG:4326"
)

# Guardar con coordenadas completas
ruta_nc = radar_to_netcdf_with_coordinates(
    radar,
    output_path="./netcdf",
    filename="radar_data.nc"
)
```

### Generación de COLMAX

```python
from radarlib.io.pyart.colmax import generate_colmax
from radarlib import config

# Generar campo COLMAX
radar_con_colmax = generate_colmax(
    radar=radar,
    elev_limit1=config.COLMAX_ELEV_LIMIT1,
    field_for_colmax="DBZH",
    RHOHV_filter=config.COLMAX_RHOHV_FILTER,
    RHOHV_umbral=config.COLMAX_RHOHV_UMBRAL,
    WRAD_filter=config.COLMAX_WRAD_FILTER,
    WRAD_umbral=config.COLMAX_WRAD_UMBRAL,
    TDR_filter=config.COLMAX_TDR_FILTER,
    TDR_umbral=config.COLMAX_TDR_UMBRAL,
    save_changes=False
)

# Verificar que COLMAX fue agregado
print("COLMAX" in radar_con_colmax.fields)  # True

# Acceder a datos COLMAX
colmax_data = radar_con_colmax.fields['COLMAX']['data']
```

### Filtros de Calidad

```python
from radarlib.io.pyart.filters import filter_fields_grc1
import pyart

# Crear GateFilter
gatefilter = pyart.correct.GateFilter(radar)

# Aplicar filtros GRC (Grupo Radar Córdoba)
gatefilter = filter_fields_grc1(
    radar,
    rhv_field="RHOHV",
    rhv_filter1=True,
    rhv_threshold1=0.55,
    wrad_field="WRAD",
    wrad_filter=True,
    wrad_threshold=4.6,
    refl_field="DBZH",
    refl_filter=True,
    refl_threshold=-3,
    zdr_field="ZDR",
    zdr_filter=True,
    zdr_threshold=8.5,
    despeckle_filter=True,
    mean_filter=True,
    mean_threshold=0.85,
    target_fields=["DBZH"],
    overwrite_fields=False
)

# Usar gatefilter en visualización
field_config = FieldPlotConfig(
    field_name="DBZH",
    gatefilter=gatefilter
)
```

---

## Utilidades

### Funciones de Nombres

```python
from radarlib.utils.names_utils import (
    extract_bufr_filename_components,
    get_netcdf_filename_from_bufr_filename,
    build_vol_types_regex,
    product_path_and_filename
)

# Extraer componentes del nombre de archivo BUFR
componentes = extract_bufr_filename_components(
    "RMA1_0315_01_DBZH_20250101T120000Z.BUFR"
)
print(componentes)
# {
#     'radar_name': 'RMA1',
#     'strategy': '0315',
#     'vol_nr': '01',
#     'field_type': 'DBZH',
#     'timestamp': '20250101T120000Z'
# }

# Generar nombre de archivo NetCDF
nombre_nc = get_netcdf_filename_from_bufr_filename(
    "RMA1_0315_01_DBZH_20250101T120000Z.BUFR"
)
print(nombre_nc)  # "RMA1_0315_01_20250101T120000Z.nc"

# Construir regex para filtrado de volúmenes
vol_types = {
    "0315": {
        "01": ["DBZH", "DBZV", "ZDR"],
        "02": ["VRAD", "WRAD"]
    }
}
regex = build_vol_types_regex(vol_types)
print(regex.pattern)
```

### Funciones de Campos

```python
from radarlib.utils.fields_utils import (
    determine_reflectivity_fields,
    get_lowest_nsweep
)

# Determinar campos de reflectividad disponibles
fields = determine_reflectivity_fields(radar)
print(f"Reflectividad horizontal: {fields['hrefl_field']}")
print(f"Reflectividad vertical: {fields['vrefl_field']}")

# Obtener índice del barrido más bajo
lowest_sweep = get_lowest_nsweep(radar)
print(f"Barrido más bajo: {lowest_sweep}")
```

---

*Continúe con el capítulo [Guía de Integración](./06_guia_integracion.md) para ver ejemplos de uso en proyectos externos.*
