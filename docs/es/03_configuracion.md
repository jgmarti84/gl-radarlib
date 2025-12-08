# 3. Configuración y Variables de Entorno

## Sistema de Configuración

radarlib utiliza un sistema de configuración flexible con múltiples niveles de precedencia:

```
1. Variables de entorno (mayor prioridad)
2. Archivo de configuración JSON (RADARLIB_CONFIG)
3. Archivo radarlib.json en el directorio del paquete
4. Valores por defecto del sistema (menor prioridad)
```

## Carga de Configuración

```python
from radarlib import config

# Obtener un valor de configuración
valor = config.get("CLAVE_CONFIGURACION")

# Obtener con valor por defecto
valor = config.get("CLAVE_CONFIGURACION", valor_por_defecto)

# Recargar configuración desde archivo específico
config.reload("/ruta/al/archivo/configuracion.json")
```

## Variables de Entorno

### Configuración General

| Variable | Descripción | Tipo | Valor por Defecto |
|----------|-------------|------|-------------------|
| `RADARLIB_CONFIG` | Ruta al archivo JSON de configuración | `str` | - |
| `BUFR_RESOURCES_PATH` | Ruta a recursos BUFR (tablas y biblioteca C) | `str` | `<paquete>/io/bufr/bufr_resources` |
| `ROOT_CACHE_PATH` | Directorio para archivos de caché | `str` | `<proyecto>/cache` |
| `ROOT_RADAR_FILES_PATH` | Directorio base para archivos de radar | `str` | `<proyecto>/data/radares` |
| `ROOT_RADAR_PRODUCTS_PATH` | Directorio para productos generados | `str` | `<proyecto>/product_output` |

### Configuración FTP

| Variable | Descripción | Tipo | Valor por Defecto |
|----------|-------------|------|-------------------|
| `FTP_HOST` | Servidor FTP para descarga de datos | `str` | `"www.example.com"` |
| `FTP_USER` | Usuario para autenticación FTP | `str` | `"example_user"` |
| `FTP_PASS` | Contraseña para autenticación FTP | `str` | `"secret"` |

### Configuración COLMAX (Máximo Columnar)

| Variable | Descripción | Tipo | Valor por Defecto |
|----------|-------------|------|-------------------|
| `COLMAX_THRESHOLD` | Umbral de reflectividad para COLMAX | `float` | `-3` |
| `COLMAX_ELEV_LIMIT1` | Límite de elevación para COLMAX | `float` | `0.65` |
| `COLMAX_RHOHV_FILTER` | Habilitar filtro RHOHV | `bool` | `True` |
| `COLMAX_RHOHV_UMBRAL` | Umbral RHOHV para filtrado | `float` | `0.8` |
| `COLMAX_WRAD_FILTER` | Habilitar filtro de ancho espectral | `bool` | `True` |
| `COLMAX_WRAD_UMBRAL` | Umbral ancho espectral | `float` | `4.6` |
| `COLMAX_TDR_FILTER` | Habilitar filtro ZDR | `bool` | `True` |
| `COLMAX_TDR_UMBRAL` | Umbral ZDR | `float` | `8.5` |

### Configuración de Visualización - Sin Filtros

Estas variables controlan la visualización de campos sin aplicar filtros de calidad.

| Variable | Descripción | Tipo | Valor por Defecto |
|----------|-------------|------|-------------------|
| `VMIN_REFL_NOFILTERS` | Valor mínimo reflectividad | `int` | `-20` |
| `VMAX_REFL_NOFILTERS` | Valor máximo reflectividad | `int` | `70` |
| `CMAP_REFL_NOFILTERS` | Mapa de color reflectividad | `str` | `"grc_th"` |
| `VMIN_RHOHV_NOFILTERS` | Valor mínimo RHOHV | `int` | `0` |
| `VMAX_RHOHV_NOFILTERS` | Valor máximo RHOHV | `int` | `1` |
| `CMAP_RHOHV_NOFILTERS` | Mapa de color RHOHV | `str` | `"grc_rho"` |
| `VMIN_VRAD_NOFILTERS` | Valor mínimo velocidad radial | `int` | `-30` |
| `VMAX_VRAD_NOFILTERS` | Valor máximo velocidad radial | `int` | `30` |
| `CMAP_VRAD_NOFILTERS` | Mapa de color velocidad | `str` | `"grc_vrad"` |
| `VMIN_ZDR_NOFILTERS` | Valor mínimo ZDR | `float` | `-7.5` |
| `VMAX_ZDR_NOFILTERS` | Valor máximo ZDR | `float` | `7.5` |
| `CMAP_ZDR_NOFILTERS` | Mapa de color ZDR | `str` | `"grc_zdr"` |
| `VMIN_PHIDP_NOFILTERS` | Valor mínimo PhiDP | `int` | `-5` |
| `VMAX_PHIDP_NOFILTERS` | Valor máximo PhiDP | `int` | `360` |
| `CMAP_PHIDP_NOFILTERS` | Mapa de color PhiDP | `str` | `"grc_th"` |
| `VMIN_KDP_NOFILTERS` | Valor mínimo KDP | `int` | `-4` |
| `VMAX_KDP_NOFILTERS` | Valor máximo KDP | `int` | `8` |
| `CMAP_KDP_NOFILTERS` | Mapa de color KDP | `str` | `"jet"` |
| `VMIN_WRAD_NOFILTERS` | Valor mínimo ancho espectral | `int` | `-2` |
| `VMAX_WRAD_NOFILTERS` | Valor máximo ancho espectral | `int` | `6` |
| `CMAP_WRAD_NOFILTERS` | Mapa de color ancho espectral | `str` | `"grc_th"` |

### Configuración de Visualización - Con Filtros

Estas variables controlan la visualización después de aplicar filtros de calidad.

| Variable | Descripción | Tipo | Valor por Defecto |
|----------|-------------|------|-------------------|
| `VMIN_REFL` | Valor mínimo reflectividad filtrada | `int` | `-20` |
| `VMAX_REFL` | Valor máximo reflectividad filtrada | `int` | `70` |
| `CMAP_REFL` | Mapa de color reflectividad | `str` | `"grc_th"` |
| `VMIN_RHOHV` | Valor mínimo RHOHV | `int` | `0` |
| `VMAX_RHOHV` | Valor máximo RHOHV | `int` | `1` |
| `CMAP_RHOHV` | Mapa de color RHOHV | `str` | `"grc_rho"` |
| `VMIN_VRAD` | Valor mínimo velocidad radial | `int` | `-15` |
| `VMAX_VRAD` | Valor máximo velocidad radial | `int` | `15` |
| `CMAP_VRAD` | Mapa de color velocidad | `str` | `"grc_vrad"` |
| `VMIN_ZDR` | Valor mínimo ZDR | `int` | `-2` |
| `VMAX_ZDR` | Valor máximo ZDR | `float` | `7.5` |
| `CMAP_ZDR` | Mapa de color ZDR | `str` | `"grc_zdr"` |
| `VMIN_PHIDP` | Valor mínimo PhiDP | `int` | `-5` |
| `VMAX_PHIDP` | Valor máximo PhiDP | `int` | `360` |
| `CMAP_PHIDP` | Mapa de color PhiDP | `str` | `"grc_th"` |
| `VMIN_KDP` | Valor mínimo KDP | `int` | `-4` |
| `VMAX_KDP` | Valor máximo KDP | `int` | `8` |
| `CMAP_KDP` | Mapa de color KDP | `str` | `"jet"` |
| `VMIN_WRAD` | Valor mínimo ancho espectral | `int` | `-2` |
| `VMAX_WRAD` | Valor máximo ancho espectral | `int` | `6` |
| `CMAP_WRAD` | Mapa de color ancho espectral | `str` | `"grc_th"` |

### Configuración de Generación de Productos

| Variable | Descripción | Tipo | Valor por Defecto |
|----------|-------------|------|-------------------|
| `FIELDS_TO_PLOT` | Campos a graficar (sin filtros) | `list` | `["DBZH", "ZDR", "COLMAX", "RHOHV"]` |
| `FILTERED_FIELDS_TO_PLOT` | Campos a graficar (con filtros) | `list` | `["DBZH", "ZDR", "COLMAX", "RHOHV", "VRAD", "WRAD", "KDP"]` |
| `PNG_DPI` | Resolución de imágenes PNG | `int` | `72` |

### Configuración de Filtros GRC (Grupo Radar Córdoba)

| Variable | Descripción | Tipo | Valor por Defecto |
|----------|-------------|------|-------------------|
| `GRC_RHV_FILTER` | Habilitar filtro RHOHV | `bool` | `True` |
| `GRC_RHV_THRESHOLD` | Umbral RHOHV | `float` | `0.55` |
| `GRC_WRAD_FILTER` | Habilitar filtro ancho espectral | `bool` | `True` |
| `GRC_WRAD_THRESHOLD` | Umbral ancho espectral | `float` | `4.6` |
| `GRC_REFL_FILTER` | Habilitar filtro reflectividad | `bool` | `True` |
| `GRC_REFL_THRESHOLD` | Umbral reflectividad | `float` | `-3` |
| `GRC_ZDR_FILTER` | Habilitar filtro ZDR | `bool` | `True` |
| `GRC_ZDR_THRESHOLD` | Umbral ZDR | `float` | `8.5` |
| `GRC_REFL_FILTER2` | Habilitar segundo filtro reflectividad | `bool` | `True` |
| `GRC_REFL_THRESHOLD2` | Segundo umbral reflectividad | `float` | `25` |
| `GRC_CM_FILTER` | Habilitar filtro clutter map | `bool` | `True` |
| `GRC_RHOHV_THRESHOLD2` | Segundo umbral RHOHV | `float` | `0.85` |
| `GRC_DESPECKLE_FILTER` | Habilitar filtro despeckle | `bool` | `True` |
| `GRC_MEAN_FILTER` | Habilitar filtro de media | `bool` | `True` |
| `GRC_MEAN_THRESHOLD` | Umbral filtro de media | `float` | `0.85` |

## Archivo de Configuración JSON

Puede crear un archivo `radarlib.json` con sus configuraciones personalizadas:

```json
{
    "FTP_HOST": "ftp.miservidor.com",
    "FTP_USER": "mi_usuario",
    "FTP_PASS": "mi_contraseña",
    "ROOT_RADAR_FILES_PATH": "/datos/radares",
    "ROOT_RADAR_PRODUCTS_PATH": "/productos/radar",
    "COLMAX_RHOHV_UMBRAL": 0.85,
    "COLMAX_WRAD_UMBRAL": 4.0,
    "PNG_DPI": 150,
    "FIELDS_TO_PLOT": ["DBZH", "VRAD", "RHOHV"],
    "GRC_RHV_THRESHOLD": 0.6
}
```

Para usar este archivo:

```bash
# Mediante variable de entorno
export RADARLIB_CONFIG="/ruta/al/radarlib.json"
python mi_script.py

# O colocar el archivo en el directorio del paquete radarlib
```

## Ejemplo de Configuración en Script

```python
import os

# Configurar variables de entorno antes de importar radarlib
os.environ['FTP_HOST'] = 'ftp.miservidor.com'
os.environ['FTP_USER'] = 'usuario'
os.environ['FTP_PASS'] = 'contraseña'
os.environ['ROOT_RADAR_FILES_PATH'] = '/datos/radares'
os.environ['COLMAX_RHOHV_UMBRAL'] = '0.85'
os.environ['PNG_DPI'] = '150'

# Ahora importar radarlib
from radarlib import config

# Verificar configuración
print(f"FTP Host: {config.FTP_HOST}")
print(f"Directorio de datos: {config.ROOT_RADAR_FILES_PATH}")
print(f"Umbral RHOHV: {config.COLMAX_RHOHV_UMBRAL}")
print(f"DPI PNG: {config.PNG_DPI}")
```

## Conversión de Tipos

El sistema de configuración convierte automáticamente los valores de variables de entorno al tipo correcto según los valores por defecto:

- Variables booleanas: `"true"`, `"1"`, `"yes"` → `True`
- Variables numéricas: Se convierten a `int` o `float` según corresponda
- Variables de texto: Se usan tal cual

```bash
# Ejemplos de configuración correcta
export COLMAX_RHOHV_FILTER=true      # Se convierte a True
export COLMAX_RHOHV_UMBRAL=0.85      # Se convierte a 0.85 (float)
export PNG_DPI=150                    # Se convierte a 150 (int)
export CMAP_REFL=viridis              # Se mantiene como "viridis" (str)
```

---

*Continúe con el capítulo [Arquitectura de Daemons](./04_arquitectura_daemons.md) para entender el sistema de servicios.*
