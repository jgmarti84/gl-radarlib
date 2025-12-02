# 1. Introducción

## Descripción General

**radarlib** es una biblioteca Python profesional desarrollada para el procesamiento, análisis y visualización de datos de radar meteorológico. Proporciona funcionalidades esenciales para la gestión de radares y productos de radar, con soporte completo para el formato BUFR utilizado por el Sistema Nacional de Radares Meteorológicos de Argentina (SiNaRaMe).

## Características Principales

### Procesamiento de Datos

- **Decodificación BUFR**: Procesamiento de archivos BUFR de radar mediante una biblioteca C optimizada
- **Integración PyART**: Conversión automática a objetos Radar de PyART para análisis avanzado
- **Exportación multi-formato**: Generación de archivos NetCDF, PNG y GeoTIFF

### Servicios de Daemons Asíncronos

- **Daemon de Descarga FTP**: Monitoreo continuo y descarga de archivos BUFR desde servidores FTP
- **Daemon de Procesamiento BUFR**: Procesamiento automático de archivos BUFR descargados a formato NetCDF
- **Daemon de Generación de Productos**: Creación automática de visualizaciones PNG y GeoTIFF
- **Daemon de Limpieza**: Gestión automática del ciclo de vida de archivos

### Visualización

- **Gráficos PPI**: Generación de plots Plan Position Indicator
- **Productos COLMAX**: Cálculo de máximos columnares con filtros polarimétricos
- **Exportación GeoTIFF**: Datos georeferenciados para sistemas GIS

## Casos de Uso

1. **Centros Operativos de Meteorología**
   - Monitoreo en tiempo real de datos de radar
   - Generación automática de productos visuales
   - Archivo de datos en formato estándar

2. **Investigación Atmosférica**
   - Análisis de eventos meteorológicos
   - Estudios de precipitación y convección
   - Validación de modelos numéricos

3. **Sistemas de Alerta Temprana**
   - Integración con sistemas de pronóstico
   - Productos automáticos para toma de decisiones
   - Monitoreo continuo 24/7

## Requisitos del Sistema

- **Python**: 3.11 o superior
- **Sistema Operativo**: Linux (probado en Ubuntu 20.04+)
- **Dependencias principales**:
  - `numpy` >= 2.0.0
  - `pandas` >= 2.0.0
  - `arm-pyart` >= 2.1.0
  - `xarray` >= 2024.0.0
  - `netcdf4` >= 1.7.0
  - `matplotlib` >= 3.9.0

## Licencia

Este proyecto está licenciado bajo la Licencia MIT. Consulte el archivo [LICENSE](../../LICENSE) para más detalles.

## Autoría

Desarrollado por el **Grupo Radar Córdoba (GRC)** - Universidad Nacional de Córdoba, Argentina.

---

*Continúe con el capítulo [Instalación](./02_instalacion.md) para comenzar a usar radarlib.*
