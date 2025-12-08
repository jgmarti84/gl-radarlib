# 2. Instalación

## Instalación desde PyPI

La forma más sencilla de instalar radarlib es mediante pip:

```bash
pip install radarlib
```

## Instalación para Desarrollo

Para contribuir al desarrollo o ejecutar la última versión desde el repositorio:

```bash
# Clonar el repositorio
git clone https://github.com/jgmarti84/radarlib.git
cd radarlib

# Crear entorno virtual (recomendado)
python -m venv venv
source venv/bin/activate  # Linux/macOS
# venv\Scripts\activate  # Windows

# Instalar dependencias de desarrollo
pip install -r requirements-dev.txt

# Instalar en modo desarrollo
pip install -e .
```

## Instalación con Poetry

Si utiliza Poetry como gestor de dependencias:

```bash
# Clonar el repositorio
git clone https://github.com/jgmarti84/radarlib.git
cd radarlib

# Instalar dependencias
poetry install

# Activar el entorno virtual
poetry shell
```

## Dependencias del Sistema

### Linux (Ubuntu/Debian)

Para la generación de documentación PDF y funcionalidades completas:

```bash
# Dependencias básicas
sudo apt-get update
sudo apt-get install -y \
    libnetcdf-dev \
    libhdf5-dev \
    libgdal-dev

# Para generación de PDF (opcional)
sudo apt-get install -y \
    pandoc \
    texlive-xetex \
    texlive-lang-spanish

# Para visualización (si no hay display)
sudo apt-get install -y xvfb
```

### macOS

```bash
# Usando Homebrew
brew install netcdf hdf5 gdal

# Para generación de PDF (opcional)
brew install pandoc
brew install --cask mactex  # Para XeLaTeX
```

## Verificación de la Instalación

Después de instalar, verifique que todo funcione correctamente:

```python
# Verificar instalación básica
import radarlib
from radarlib import config

print(f"radarlib instalado correctamente")
print(f"BUFR Resources Path: {config.BUFR_RESOURCES_PATH}")

# Verificar módulos principales
from radarlib.io.bufr.bufr import bufr_to_dict
from radarlib.io.ftp import FTPClient
from radarlib.daemons import DaemonManager

print("Todos los módulos cargados correctamente")
```

## Estructura del Paquete Instalado

Una vez instalado, radarlib tiene la siguiente estructura:

```
radarlib/
├── __init__.py          # Inicialización del paquete
├── config.py            # Sistema de configuración
├── colormaps.py         # Mapas de color personalizados
├── pyart_defaults.py    # Configuración por defecto de PyART
├── resources.py         # Gestión de recursos
├── daemons/             # Servicios daemon
│   ├── download_daemon.py
│   ├── processing_daemon.py
│   ├── product_daemon.py
│   ├── cleanup_daemon.py
│   └── manager.py
├── io/                  # Entrada/Salida
│   ├── bufr/           # Procesamiento BUFR
│   ├── ftp/            # Cliente FTP
│   └── pyart/          # Integración PyART
├── state/               # Seguimiento de estado
│   └── sqlite_tracker.py
└── utils/               # Utilidades
    ├── fields_utils.py
    └── names_utils.py
```

## Solución de Problemas Comunes

### Error: libdecbufr.so no encontrada

```bash
# La biblioteca C debe estar incluida en el paquete
# Verifique que esté en:
ls $(python -c "import radarlib; print(radarlib.config.BUFR_RESOURCES_PATH)")/dynamic_library/
```

### Error: PyART no inicializa correctamente

```python
# Configure la variable de entorno para modo silencioso
import os
os.environ['PYART_QUIET'] = '1'

import radarlib  # Ahora no mostrará el banner
```

### Problemas con matplotlib en servidor sin display

```bash
# Usar backend Agg antes de importar
export MPLBACKEND=Agg

# O en Python:
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
```

---

*Continúe con el capítulo [Configuración y Variables de Entorno](./03_configuracion.md) para personalizar radarlib.*
