#!/bin/bash
# -*- coding: utf-8 -*-
#
# Script para generar documentación PDF de radarlib
# 
# Uso: ./generate_pdf.sh
#
# Requisitos:
#   - pandoc (apt install pandoc)
#   - texlive-xetex (apt install texlive-xetex)
#   - texlive-lang-spanish (apt install texlive-lang-spanish)
#
# Alternativamente, puede usar la salida HTML si no tiene LaTeX instalado.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCS_DIR="$SCRIPT_DIR/es"
OUTPUT_PDF="$DOCS_DIR/radarlib_documentacion.pdf"
OUTPUT_HTML="$DOCS_DIR/radarlib_documentacion.html"

echo "================================================"
echo "  Generador de Documentación PDF - radarlib    "
echo "================================================"
echo ""

# Verificar que existe el directorio de documentación
if [ ! -d "$DOCS_DIR" ]; then
    echo "Error: Directorio de documentación no encontrado: $DOCS_DIR"
    exit 1
fi

# Lista ordenada de archivos Markdown
MD_FILES=(
    "$DOCS_DIR/01_introduccion.md"
    "$DOCS_DIR/02_instalacion.md"
    "$DOCS_DIR/03_configuracion.md"
    "$DOCS_DIR/04_arquitectura_daemons.md"
    "$DOCS_DIR/05_modulos_principales.md"
    "$DOCS_DIR/06_guia_integracion.md"
    "$DOCS_DIR/07_referencia_api.md"
    "$DOCS_DIR/08_ejemplos_avanzados.md"
)

# Verificar que existen todos los archivos
for file in "${MD_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "Advertencia: Archivo no encontrado: $file"
    fi
done

# Verificar si pandoc está instalado
if ! command -v pandoc &> /dev/null; then
    echo "Error: pandoc no está instalado."
    echo ""
    echo "Para instalar en Ubuntu/Debian:"
    echo "  sudo apt install pandoc"
    echo ""
    echo "Para instalar en macOS:"
    echo "  brew install pandoc"
    echo ""
    exit 1
fi

# Intentar generar PDF
echo "Generando documentación PDF..."
echo ""

if command -v xelatex &> /dev/null; then
    # Usar XeLaTeX para mejor soporte de Unicode
    pandoc "${MD_FILES[@]}" \
        -o "$OUTPUT_PDF" \
        --from markdown \
        --toc \
        --toc-depth=3 \
        -V geometry:margin=1in \
        -V fontsize=11pt \
        -V lang=es-419 \
        -V mainfont="DejaVu Serif" \
        -V monofont="DejaVu Sans Mono" \
        -V documentclass=report \
        -V colorlinks=true \
        -V linkcolor=blue \
        -V urlcolor=blue \
        --pdf-engine=xelatex \
        --highlight-style=tango \
        --metadata title="Documentación de radarlib" \
        --metadata author="Grupo Radar Córdoba (GRC)" \
        --metadata date="$(date +%Y-%m-%d)"
    
    echo "✅ PDF generado exitosamente: $OUTPUT_PDF"
else
    echo "⚠️ XeLaTeX no está instalado. Generando HTML en su lugar..."
    echo ""
    
    # Generar HTML como alternativa
    pandoc "${MD_FILES[@]}" \
        -o "$OUTPUT_HTML" \
        --from markdown \
        --to html5 \
        --toc \
        --toc-depth=3 \
        --standalone \
        --highlight-style=tango \
        --metadata title="Documentación de radarlib" \
        --metadata author="Grupo Radar Córdoba (GRC)" \
        --metadata date="$(date +%Y-%m-%d)" \
        -c "https://cdn.simplecss.org/simple.min.css"
    
    echo "✅ HTML generado: $OUTPUT_HTML"
    echo ""
    echo "Para generar PDF, instale XeLaTeX:"
    echo "  sudo apt install texlive-xetex texlive-lang-spanish texlive-fonts-recommended"
fi

echo ""
echo "Proceso completado."
