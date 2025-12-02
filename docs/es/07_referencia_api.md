# 7. Referencia de API

## Módulo `radarlib.config`

### Funciones

#### `get(key: str, default: Any = None) -> Any`

Obtiene un valor de configuración.

**Parámetros:**
- `key` (str): Clave de configuración
- `default` (Any): Valor por defecto si la clave no existe

**Retorna:**
- Valor de configuración o el valor por defecto

**Ejemplo:**
```python
from radarlib import config

ftp_host = config.get("FTP_HOST")
custom_val = config.get("MI_CLAVE", "valor_defecto")
```

#### `reload(path: Optional[str] = None) -> None`

Recarga la configuración desde archivo.

**Parámetros:**
- `path` (str, opcional): Ruta al archivo de configuración JSON

**Ejemplo:**
```python
config.reload("/ruta/al/config.json")
```

---

## Módulo `radarlib.io.bufr.bufr`

### Funciones Principales

#### `bufr_to_dict(bufr_filename: str, root_resources: str | None = None, legacy: bool = False) -> Optional[dict]`

Decodifica un archivo BUFR y devuelve un diccionario con los datos.

**Parámetros:**
- `bufr_filename` (str): Ruta al archivo BUFR
- `root_resources` (str, opcional): Ruta a recursos BUFR (tablas, biblioteca C)
- `legacy` (bool): Si True, usa formato legacy (sin DataFrame de sweeps)

**Retorna:**
- `dict` con claves `'data'` e `'info'`, o `None` si falla

**Ejemplo:**
```python
from radarlib.io.bufr.bufr import bufr_to_dict

resultado = bufr_to_dict("archivo.BUFR")
if resultado:
    data = resultado['data']  # numpy array
    info = resultado['info']  # diccionario de metadatos
```

#### `dec_bufr_file(bufr_filename: str, root_resources: str | None = None, parallel: bool = True) -> Tuple[Dict, List[dict], np.ndarray, List]`

Decodifica un archivo BUFR con acceso a datos internos.

**Parámetros:**
- `bufr_filename` (str): Ruta al archivo BUFR
- `root_resources` (str, opcional): Ruta a recursos
- `parallel` (bool): Descompresión paralela de barridos

**Retorna:**
- Tupla: `(meta_vol, sweeps, vol_data, run_log)`

#### `bufr_name_metadata(bufr_filename: str) -> dict`

Extrae metadatos del nombre de archivo BUFR.

**Parámetros:**
- `bufr_filename` (str): Nombre del archivo BUFR

**Retorna:**
- Dict con: `radar_name`, `estrategia_nombre`, `estrategia_nvol`, `tipo_producto`, `filename`

**Ejemplo:**
```python
meta = bufr_name_metadata("RMA1_0315_01_DBZH_20250101T120000Z.BUFR")
# {'radar_name': 'RMA1', 'estrategia_nombre': '0315', ...}
```

### Context Managers

#### `decbufr_library_context(root_resources: str | None = None)`

Context manager para la biblioteca C de decodificación.

**Ejemplo:**
```python
with decbufr_library_context() as lib:
    meta = get_metadata(lib, "archivo.BUFR")
```

---

## Módulo `radarlib.io.bufr.bufr_to_pyart`

### Funciones

#### `bufr_fields_to_pyart_radar(fields: List[dict], *, include_scan_metadata: bool = False, root_scan_config_files: Optional[Path] = None, config: Optional[Dict] = None) -> Radar`

Convierte campos BUFR decodificados a objeto PyART Radar.

**Parámetros:**
- `fields` (List[dict]): Lista de volúmenes decodificados por `bufr_to_dict`
- `include_scan_metadata` (bool): Incluir metadatos de escaneo XML
- `root_scan_config_files` (Path, opcional): Ruta a archivos de configuración
- `config` (Dict, opcional): Configuración personalizada

**Retorna:**
- Objeto `pyart.core.Radar`

**Ejemplo:**
```python
from radarlib.io.bufr.bufr_to_pyart import bufr_fields_to_pyart_radar

vol1 = bufr_to_dict("DBZH.BUFR")
vol2 = bufr_to_dict("VRAD.BUFR")
radar = bufr_fields_to_pyart_radar([vol1, vol2])
```

#### `bufr_paths_to_pyart(bufr_paths: List[str], *, root_resources: Optional[str] = None, save_path: Optional[Path] = None) -> Radar`

Decodifica y convierte archivos BUFR a PyART en un solo paso.

**Parámetros:**
- `bufr_paths` (List[str]): Rutas a archivos BUFR
- `root_resources` (str, opcional): Ruta a recursos
- `save_path` (Path, opcional): Directorio para guardar NetCDF

**Retorna:**
- Objeto `pyart.core.Radar`

#### `save_radar_to_cfradial(radar: Radar, out_file: Path, format: str = "NETCDF4") -> Path`

Guarda objeto Radar en formato NetCDF CFRadial.

**Parámetros:**
- `radar`: Objeto PyART Radar
- `out_file` (Path): Ruta del archivo de salida
- `format` (str): Formato NetCDF

**Retorna:**
- Path del archivo guardado

---

## Módulo `radarlib.io.ftp`

### Clase `RadarFTPClient`

#### Constructor

```python
RadarFTPClient(
    host: str,
    user: str,
    password: str,
    base_dir: str = "L2",
    timeout: int = 30
)
```

#### Métodos

##### `list_dir(remote_path: str) -> List[str]`

Lista contenido de directorio remoto.

##### `download_file(remote_path: str, local_path: Path) -> Path`

Descarga un archivo del servidor FTP.

##### `traverse_radar(radar_name: str, dt_start: datetime = None, dt_end: datetime = None, include_start: bool = True, include_end: bool = True, vol_types: Optional[dict] = None) -> Generator`

Recorre estructura de directorios del radar.

**Retorna:**
- Generator de tuplas `(datetime, filename, remote_path)`

**Ejemplo:**
```python
with RadarFTPClient(host, user, password) as client:
    for dt, fname, path in client.traverse_radar("RMA1", dt_start, dt_end):
        print(f"{dt}: {fname}")
```

### Clase `RadarFTPClientAsync`

Versión asíncrona de `RadarFTPClient`.

#### Métodos Adicionales

##### `async download_file_async(remote_path: Path, local_path: Path) -> Path`

Descarga asíncrona de archivo.

##### `async download_files_parallel(files: List[Tuple[Path, Path]]) -> List[Path]`

Descarga múltiples archivos en paralelo.

**Ejemplo:**
```python
async with RadarFTPClientAsync(host, user, password, max_workers=5) as client:
    archivos = [(Path(remote1), Path(local1)), (Path(remote2), Path(local2))]
    await client.download_files_parallel(archivos)
```

---

## Módulo `radarlib.daemons`

### Clase `DaemonManager`

#### Constructor

```python
DaemonManager(config: DaemonManagerConfig)
```

#### Métodos

##### `async start() -> None`

Inicia todos los daemons habilitados.

##### `stop() -> None`

Detiene todos los daemons.

##### `async restart_download_daemon(new_config: Optional[Dict] = None) -> None`

Reinicia el daemon de descarga con nueva configuración.

##### `async restart_processing_daemon(new_config: Optional[Dict] = None) -> None`

Reinicia el daemon de procesamiento.

##### `get_status() -> Dict`

Obtiene estado de todos los daemons.

**Retorna:**
```python
{
    'manager_running': bool,
    'radar_code': str,
    'base_path': str,
    'download_daemon': {'enabled': bool, 'running': bool, 'stats': dict},
    'processing_daemon': {'enabled': bool, 'running': bool, 'stats': dict},
    'product_daemon': {'enabled': bool, 'running': bool, 'stats': dict},
    'cleanup_daemon': {'enabled': bool, 'running': bool, 'stats': dict}
}
```

### Clase `DaemonManagerConfig`

```python
@dataclass
class DaemonManagerConfig:
    radar_name: str
    base_path: Path
    ftp_host: str
    ftp_user: str
    ftp_password: str
    ftp_base_path: str
    volume_types: Dict
    start_date: Optional[datetime] = None
    download_poll_interval: int = 60
    processing_poll_interval: int = 30
    product_poll_interval: int = 30
    cleanup_poll_interval: int = 1800
    enable_download_daemon: bool = True
    enable_processing_daemon: bool = True
    enable_product_daemon: bool = True
    enable_cleanup_daemon: bool = False
    product_type: str = "image"
    add_colmax: bool = True
    bufr_retention_days: int = 7
    netcdf_retention_days: int = 7
```

---

## Módulo `radarlib.io.pyart.colmax`

### Función Principal

#### `generate_colmax(radar: Radar, elev_limit1: float = 0, field_for_colmax: str = "TH", RHOHV_filter: bool = False, RHOHV_umbral: float = 0.9, WRAD_filter: bool = False, WRAD_umbral: float = 4.2, TDR_filter: bool = False, TDR_umbral: float = 8.5, save_changes: bool = False, path_out: Optional[str] = None) -> Radar`

Genera campo COLMAX (máximo columnar).

**Parámetros:**
- `radar` (Radar): Objeto PyART Radar
- `elev_limit1` (float): Límite de elevación inferior
- `field_for_colmax` (str): Campo fuente para COLMAX
- `RHOHV_filter` (bool): Aplicar filtro RHOHV
- `RHOHV_umbral` (float): Umbral RHOHV
- `WRAD_filter` (bool): Aplicar filtro ancho espectral
- `WRAD_umbral` (float): Umbral ancho espectral
- `TDR_filter` (bool): Aplicar filtro ZDR
- `TDR_umbral` (float): Umbral ZDR
- `save_changes` (bool): Guardar radar modificado
- `path_out` (str): Ruta de salida si save_changes=True

**Retorna:**
- Nuevo objeto Radar con campo COLMAX agregado

---

## Módulo `radarlib.io.pyart.radar_png_plotter`

### Clases

#### `RadarPlotConfig`

```python
RadarPlotConfig(
    figsize: Tuple[int, int] = (12, 12),
    dpi: int = 150,
    transparent: bool = True,
    colorbar: bool = False,
    title: bool = False,
    axis_labels: bool = False,
    tight_layout: bool = True
)
```

#### `FieldPlotConfig`

```python
FieldPlotConfig(
    field_name: str,
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    cmap: Optional[str] = None,
    sweep: Optional[int] = None,
    gatefilter: Optional[GateFilter] = None
)
```

### Funciones

#### `plot_ppi_field(radar: Radar, field: str, sweep: int = None, config: RadarPlotConfig = None, field_config: FieldPlotConfig = None) -> Tuple[Figure, Axes]`

Crea gráfico PPI para un campo.

#### `save_ppi_png(fig: Figure, output_path: str, filename: str, dpi: int = 150, transparent: bool = True) -> str`

Guarda figura como PNG.

#### `plot_and_save_ppi(radar: Radar, field: str, output_path: str, filename: str, ...) -> str`

Crea y guarda gráfico PPI en un paso.

#### `plot_multiple_fields(radar: Radar, fields: List[str], output_base_path: str, ...) -> Dict[str, str]`

Genera gráficos para múltiples campos.

#### `export_fields_to_geotiff(radar: Radar, fields: List[str], output_base_path: str, sweep: int = 0, crs: str = "EPSG:4326") -> Dict[str, str]`

Exporta campos como GeoTIFF georeferenciados.

---

## Módulo `radarlib.io.pyart.radar_geotiff_exporter`

### Funciones

#### `save_ppi_field_to_geotiff(radar: Radar, field: str, output_path: str, filename: str, sweep: int = 0, crs: str = "EPSG:4326") -> str`

Guarda campo como GeoTIFF.

#### `save_multiple_fields_to_geotiff(radar: Radar, fields: List[str], output_base_path: str, sweep: int = 0, crs: str = "EPSG:4326") -> Dict[str, str]`

Exporta múltiples campos como GeoTIFF.

#### `radar_to_netcdf_with_coordinates(radar: Radar, output_path: str, filename: str = "radar_data.nc") -> str`

Guarda radar con coordenadas completas.

---

## Módulo `radarlib.utils.names_utils`

### Funciones

#### `extract_bufr_filename_components(filename: str) -> Dict[str, str]`

Extrae componentes del nombre de archivo BUFR.

**Retorna:**
```python
{
    'radar_name': str,
    'strategy': str,
    'vol_nr': str,
    'field_type': str,
    'timestamp': str
}
```

#### `get_netcdf_filename_from_bufr_filename(bufr_filename: str) -> str`

Genera nombre de archivo NetCDF desde nombre BUFR.

#### `build_vol_types_regex(vol_types: Dict) -> re.Pattern`

Construye patrón regex para filtrado de tipos de volumen.

#### `product_path_and_filename(radar: Radar, field: str, sweep: int, round_filename: bool = True, filtered: bool = False) -> Dict`

Genera rutas y nombres para productos.

---

## Módulo `radarlib.utils.fields_utils`

### Funciones

#### `determine_reflectivity_fields(radar: Radar) -> Dict[str, str]`

Determina campos de reflectividad disponibles.

**Retorna:**
```python
{
    'hrefl_field': str,      # Reflectividad horizontal
    'hrefl_field_raw': str,  # Reflectividad H sin filtrar
    'vrefl_field': str,      # Reflectividad vertical
    'vrefl_field_raw': str   # Reflectividad V sin filtrar
}
```

#### `get_lowest_nsweep(radar: Radar) -> int`

Obtiene índice del barrido con elevación más baja.

---

*Continúe con el capítulo [Ejemplos Avanzados](./08_ejemplos_avanzados.md) para casos de uso especializados.*
