#!/bin/bash
# -*- coding: utf-8 -*-
#
# Script para generar documentación PDF de radarlib
#
# Uso:
#   ./generate_pdf.sh          # genera ambas versiones (EN + ES)
#   ./generate_pdf.sh en       # genera solo la versión en inglés
#   ./generate_pdf.sh es       # genera solo la versión en español (secciones separadas)
#   ./generate_pdf.sh es-single # genera solo la versión en español (README.es.md)
#
# Requisitos:
#   - pandoc (apt install pandoc)
#   - texlive-xetex (apt install texlive-xetex)
#   - texlive-lang-spanish (apt install texlive-lang-spanish)
#
# Alternativamente, puede usar la salida HTML si no tiene LaTeX instalado.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LANG_ARG="${1:-all}"
VERSION="v0.1.0"

# ─── Helper: detect best available font ──────────────────────────────────────
best_font() {
    local preferred="$1"
    local fallback="$2"
    # Use exact family-name match (colon-delimited fc-list output) to avoid
    # substring false-positives like "DejaVu Serif" matching "DejaVu Serif Condensed".
    # Return empty if neither preferred nor fallback are detected so the caller
    # can avoid passing a non-existent font to fontspec (which causes errors).
    if fc-list 2>/dev/null | grep -E "(^|:) *${preferred} *(:|$)" -q; then
        echo "$preferred"
    elif [ -n "$fallback" ] && fc-list 2>/dev/null | grep -E "(^|:) *${fallback} *(:|$)" -q; then
        echo "$fallback"
    else
        echo ""
    fi
}

# ─── Helper: generate PDF or HTML fallback ───────────────────────────────────
generate_doc() {
    local output_pdf="$1"
    local output_html="$2"
    local lang_code="$3"      # e.g. "en" or "es-419"
    local title="$4"
    shift 4
    local md_files=("$@")

    echo "  → Archivos de entrada:"
    for f in "${md_files[@]}"; do
        if [ -f "$f" ]; then
            echo "      $f"
        else
            echo "      [FALTANTE] $f"
        fi
    done
    echo ""

    if command -v xelatex &> /dev/null; then
        local main_font mono_font
        main_font="$(best_font "DejaVu Serif" "Latin Modern Roman")"
        mono_font="$(best_font "DejaVu Sans Mono" "Latin Modern Mono")"

        # Build pandoc argument array and only include font settings when
        # the fonts were actually detected. This prevents fontspec errors from
        # XeLaTeX when a requested font isn't available on the system.
        pandoc_args=(
            "${md_files[@]}"
            -o "$output_pdf"
            --from markdown
            --toc
            --toc-depth=3
            -V geometry:margin=1in
            -V fontsize=11pt
            -V lang="$lang_code"
            -V documentclass=report
            -V colorlinks=true
            -V linkcolor=blue
            -V urlcolor=blue
            --pdf-engine=xelatex
            --highlight-style=tango
            --metadata title="$title"
            --metadata author="Grupo Radar Córdoba (GRC)"
            --metadata date="$VERSION"
        )

        if [ -n "$main_font" ]; then
            pandoc_args+=( -V mainfont="$main_font" )
        fi
        if [ -n "$mono_font" ]; then
            pandoc_args+=( -V monofont="$mono_font" )
        fi

        pandoc "${pandoc_args[@]}"

        echo "  ✅ PDF generado: $output_pdf"
    else
        echo "  ⚠️  XeLaTeX no disponible — generando HTML en su lugar..."

        pandoc "${md_files[@]}" \
            -o "$output_html" \
            --from markdown \
            --to html5 \
            --toc \
            --toc-depth=3 \
            --standalone \
            --highlight-style=tango \
            --metadata title="$title" \
            --metadata author="Grupo Radar Córdoba (GRC)" \
            --metadata date="$VERSION" \
            -c "https://cdn.simplecss.org/simple.min.css"

        echo "  ✅ HTML generado: $output_html"
        echo "  Para generar PDF instale: sudo apt install texlive-xetex texlive-lang-spanish texlive-fonts-recommended"
    fi
}

# ─── Verify pandoc ────────────────────────────────────────────────────────────
if ! command -v pandoc &> /dev/null; then
    echo "Error: pandoc no está instalado."
    echo ""
    echo "Para instalar en Ubuntu/Debian:  sudo apt install pandoc"
    echo "Para instalar en macOS:          brew install pandoc"
    exit 1
fi

echo "========================================================"
echo "   Generador de Documentación PDF/HTML — radarlib      "
echo "========================================================"
echo ""

# ─── English version (docs/README.md → docs/radarlib_documentation.pdf) ──────
generate_en() {
    local en_src="$SCRIPT_DIR/README.md"
    local out_pdf="$SCRIPT_DIR/radarlib_documentation.pdf"
    local out_html="$SCRIPT_DIR/radarlib_documentation.html"

    echo "[ EN ] Generando documentación en inglés..."
    if [ ! -f "$en_src" ]; then
        echo "  Error: $en_src no encontrado."
        return 1
    fi
    generate_doc "$out_pdf" "$out_html" "en" "radarlib Documentation" "$en_src"
}

# ─── Spanish single-file version (docs/README.es.md → docs/radarlib_documentacion_es.pdf) ──
generate_es_single() {
    local es_src="$SCRIPT_DIR/README.es.md"
    local out_pdf="$SCRIPT_DIR/radarlib_documentacion_es.pdf"
    local out_html="$SCRIPT_DIR/radarlib_documentacion_es.html"

    echo "[ ES ] Generando documentación en español (archivo único)..."
    if [ ! -f "$es_src" ]; then
        echo "  Error: $es_src no encontrado."
        return 1
    fi
    generate_doc "$out_pdf" "$out_html" "es-419" "Documentación de radarlib" "$es_src"
}

# ─── Spanish multi-file version (docs/es/*.md → docs/es/radarlib_documentacion.pdf) ──
generate_es_multi() {
    local es_dir="$SCRIPT_DIR/es"
    local out_pdf="$es_dir/radarlib_documentacion.pdf"
    local out_html="$es_dir/radarlib_documentacion.html"

    local md_files=(
        "$es_dir/01_introduccion.md"
        "$es_dir/02_instalacion.md"
        "$es_dir/03_configuracion.md"
        "$es_dir/04_arquitectura_daemons.md"
        "$es_dir/05_modulos_principales.md"
        "$es_dir/06_guia_integracion.md"
        "$es_dir/07_referencia_api.md"
        "$es_dir/08_ejemplos_avanzados.md"
    )

    echo "[ ES ] Generando documentación en español (secciones separadas)..."
    if [ ! -d "$es_dir" ]; then
        echo "  Error: directorio $es_dir no encontrado."
        return 1
    fi
    generate_doc "$out_pdf" "$out_html" "es-419" "Documentación de radarlib" "${md_files[@]}"
}

# ─── Dispatch ─────────────────────────────────────────────────────────────────
case "$LANG_ARG" in
    en)
        generate_en
        ;;
    es)
        generate_es_multi
        ;;
    es-single)
        generate_es_single
        ;;
    all)
        generate_en
        echo ""
        generate_es_single
        echo ""
        generate_es_multi
        ;;
    *)
        echo "Argumento no reconocido: '$LANG_ARG'"
        echo "Uso: $0 [en|es|es-single|all]"
        echo "  en        — documentación en inglés (docs/README.md)"
        echo "  es-single — documentación en español, archivo único (docs/README.es.md)"
        echo "  es        — documentación en español, secciones separadas (docs/es/*.md)"
        echo "  all       — todas las versiones (valor por defecto)"
        exit 1
        ;;
esac

echo ""
echo "Proceso completado."
