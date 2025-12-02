# Documentación de radarlib

## Tabla de Contenidos

1. [Introducción](./01_introduccion.md)
2. [Instalación](./02_instalacion.md)
3. [Configuración y Variables de Entorno](./03_configuracion.md)
4. [Arquitectura de Daemons](./04_arquitectura_daemons.md)
5. [Módulos Principales](./05_modulos_principales.md)
   - [Cliente FTP](./05_modulos_principales.md#cliente-ftp)
   - [Procesamiento BUFR](./05_modulos_principales.md#procesamiento-bufr)
   - [Integración PyART](./05_modulos_principales.md#integración-pyart)
   - [Visualización](./05_modulos_principales.md#visualización)
6. [Guía de Integración](./06_guia_integracion.md)
7. [Referencia de API](./07_referencia_api.md)
8. [Ejemplos Avanzados](./08_ejemplos_avanzados.md)

## Generación de PDF

Esta documentación puede ser generada como PDF utilizando:

```bash
# Usando pandoc (recomendado)
./generate_pdf.sh

# O manualmente
pandoc docs/es/*.md -o docs/es/radarlib_documentacion.pdf \
    --toc --toc-depth=3 \
    -V geometry:margin=1in \
    -V lang=es-419 \
    --pdf-engine=xelatex
```

## Acerca de

**radarlib** es una biblioteca Python profesional para el procesamiento y visualización de datos de radar meteorológico. Diseñada para trabajar con datos BUFR y generar productos en formatos estándar (NetCDF, PNG, GeoTIFF).

---

*Documentación en Español (Latinoamérica) - Grupo Radar Córdoba (GRC)*
