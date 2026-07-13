#!/bin/bash
# -*- coding: utf-8 -*-
#
# Script para generar documentación PDF de radarlib
#
# Uso:
#   ./generate_pdf.sh              # genera ambas versiones (EN + ES) - MÁSTER
#   ./generate_pdf.sh EN           # genera solo versión en inglés - MÁSTER
#   ./generate_pdf.sh ES           # genera solo versión en español - MÁSTER
#   ./generate_pdf.sh en           # genera versión en inglés (legacy - README.md)
#   ./generate_pdf.sh es           # genera versión en español (legacy - secciones separadas)
#   ./generate_pdf.sh es-single    # genera versión en español (legacy - README.es.md)
#
# Requisitos:
#   - pandoc (apt install pandoc)
#   - texlive-xetex (apt install texlive-xetex)
#   - texlive-lang-spanish (apt install texlive-lang-spanish)
#
# Alternativamente, puede usar la salida HTML si no tiene LaTeX instalado.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LANG_ARG="${1:-master}"
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

# ─── Helper: preprocess markdown for PDF/HTML rendering ─────────────────────
preprocess_markdown_file() {
    local input_file="$1"
    local output_file="$2"

    python3 - "$input_file" "$output_file" <<'PY'
import re
import sys
from pathlib import Path

input_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])

lines = input_path.read_text(encoding="utf-8").splitlines(keepends=True)

CHECKMARK_PATTERNS = [
    r"\[CHECK\]",
    r":white_check_mark:",
    r"✅",
    r"✔️",
    r"✔",
    r"☑️",
    r"☑",
    r"✓",
]

connector_line_re = re.compile(r"^\s*(\|.*|[vV])\s*$")
node_line_re = re.compile(r"^\s*\[.+\].*$")
key_functions_header_re = re.compile(r"^\s*\*\*(Funciones Clave|Key Functions):\*\*\s*$")
bullet_line_re = re.compile(r"^\s*-\s+")


def strip_checkmarks(text: str) -> str:
    cleaned = text
    for pattern in CHECKMARK_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned)
    cleaned = re.sub(r"^\s*[-*+]\s{2,}", "- ", cleaned)
    return cleaned


def is_flow_block(block_lines: list[str]) -> bool:
    if len(block_lines) < 4:
        return False

    connectors = sum(1 for line in block_lines if connector_line_re.match(line.rstrip("\n")))
    nodes = sum(1 for line in block_lines if node_line_re.match(line.rstrip("\n")))
    has_direction = any("--" in line or "->" in line or "→" in line for line in block_lines)

    return connectors >= 2 and nodes >= 1 and has_direction


def transform_block(block_lines: list[str]) -> list[str]:
    if is_flow_block(block_lines):
        return ["```text\n", *block_lines, "```\n"]
    return block_lines


SECTIONS_TO_STRIP = re.compile(
    r"^## ("
    r"Known Gaps & Risks"
    r"|Brechas Conocidas & Riesgos"
    r"|SDD Workflow — Follow This Every Time"
    r"|Flujo de Trabajo SDD — Seguir Esto Siempre"
    r"|Contributing"
    r"|Contribuyendo"
    r"|License"
    r"|Licencia"
    r"|Acknowledgments"
    r"|Reconocimientos"
    r")\s*$"
)
section_header_re = re.compile(r"^## ")


def strip_sections(lines: list[str]) -> list[str]:
    """Drop entire ## sections whose title matches SECTIONS_TO_STRIP."""
    result: list[str] = []
    skipping = False

    for line in lines:
        if section_header_re.match(line):
            if SECTIONS_TO_STRIP.match(line):
                skipping = True
                continue
            else:
                skipping = False

        if not skipping:
            result.append(line)

    return result


def wrap_key_function_blocks(lines: list[str]) -> list[str]:
    wrapped: list[str] = []
    inside_fence = False
    index = 0

    while index < len(lines):
        line = lines[index]
        stripped = line.lstrip()

        if stripped.startswith("```") or stripped.startswith("~~~"):
            inside_fence = not inside_fence
            wrapped.append(line)
            index += 1
            continue

        if not inside_fence and key_functions_header_re.match(line.rstrip("\n")):
            wrapped.append(line)
            look_ahead = index + 1
            bullet_block: list[str] = []

            while look_ahead < len(lines) and bullet_line_re.match(lines[look_ahead]):
                bullet_block.append(lines[look_ahead])
                look_ahead += 1

            if bullet_block:
                wrapped.append("```text\n")
                wrapped.extend(bullet_block)
                wrapped.append("```\n")
                index = look_ahead
                continue

        wrapped.append(line)
        index += 1

    return wrapped


output_lines: list[str] = []
paragraph_buffer: list[str] = []
inside_fence = False

cleaned_lines = [strip_checkmarks(original_line) for original_line in lines]
cleaned_lines = strip_sections(cleaned_lines)
cleaned_lines = wrap_key_function_blocks(cleaned_lines)

for line in cleaned_lines:
    stripped = line.lstrip()

    if stripped.startswith("```") or stripped.startswith("~~~"):
        if paragraph_buffer:
            output_lines.extend(transform_block(paragraph_buffer))
            paragraph_buffer = []
        inside_fence = not inside_fence
        output_lines.append(line)
        continue

    if inside_fence:
        output_lines.append(line)
        continue

    if line.strip() == "":
        if paragraph_buffer:
            output_lines.extend(transform_block(paragraph_buffer))
            paragraph_buffer = []
        output_lines.append(line)
        continue

    paragraph_buffer.append(line)

if paragraph_buffer:
    output_lines.extend(transform_block(paragraph_buffer))

output_path.write_text("".join(output_lines), encoding="utf-8")
PY
}

# ─── Helper: generate PDF or HTML fallback ───────────────────────────────────
generate_doc() {
    local output_pdf="$1"
    local output_html="$2"
    local lang_code="$3"      # e.g. "en" or "es-419"
    local title="$4"
    shift 4
    local md_files=("$@")
    local tmp_dir
    local preprocessed_files=()

    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' RETURN

    echo "  → Archivos de entrada:"
    for f in "${md_files[@]}"; do
        if [ -f "$f" ]; then
            echo "      $f"
        else
            echo "      [FALTANTE] $f"
        fi
    done
    echo ""

    for f in "${md_files[@]}"; do
        local tmp_file
        tmp_file="$tmp_dir/$(basename "$f")"
        preprocess_markdown_file "$f" "$tmp_file"
        preprocessed_files+=("$tmp_file")
    done

    if command -v xelatex &> /dev/null; then
        local main_font mono_font
        main_font="$(best_font "DejaVu Serif" "Latin Modern Roman")"
        mono_font="$(best_font "DejaVu Sans Mono" "Latin Modern Mono")"

        # Build pandoc argument array and only include font settings when
        # the fonts were actually detected. This prevents fontspec errors from
        # XeLaTeX when a requested font isn't available on the system.
        pandoc_args=(
            "${preprocessed_files[@]}"
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

        # LaTeX header: enable line-breaking inside highlighted code blocks.
        # fvextra redefines the fancyvrb Highlighting environment with breaklines
        # so long lines with comments don't overflow the page.
        local header_path="$SCRIPT_DIR/latex/code_block_wrap.tex"
        if [ -f "$header_path" ]; then
            pandoc_args+=( -H "$header_path" )
        fi

        # Lua filter: make inline code in table cells line-breakable.
        # Without this, long \texttt{} strings (env var names, paths) overflow
        # column boundaries because LaTeX has no break points after \_ or /.
        local filter_path="$SCRIPT_DIR/filters/breakable_table_code.lua"
        if [ -f "$filter_path" ]; then
            pandoc_args+=( --lua-filter "$filter_path" )
        fi

        pandoc "${pandoc_args[@]}"

        echo "  ✅ PDF generado: $output_pdf"
    else
        echo "  ⚠️  XeLaTeX no disponible — generando HTML en su lugar..."

        pandoc "${preprocessed_files[@]}" \
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

    echo "[ EN ] Generando documentación en inglés (LEGACY)..."
    if [ ! -f "$en_src" ]; then
        echo "  Error: $en_src no encontrado."
        return 1
    fi
    generate_doc "$out_pdf" "$out_html" "en" "radarlib Documentation (Legacy)" "$en_src"
}

# ─── English MASTER version (docs/radarlib_EN.md → docs/radarlib_EN.pdf) ──────
generate_en_master() {
    local en_src="$SCRIPT_DIR/radarlib_EN.md"
    local out_pdf="$SCRIPT_DIR/radarlib_EN.pdf"
    local out_html="$SCRIPT_DIR/radarlib_EN.html"

    echo "[ EN ] Generando documentación en inglés (MÁSTER)..."
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

    echo "[ ES ] Generando documentación en español (LEGACY - archivo único)..."
    if [ ! -f "$es_src" ]; then
        echo "  Error: $es_src no encontrado."
        return 1
    fi
    generate_doc "$out_pdf" "$out_html" "es-419" "Documentación de radarlib (Legacy)" "$es_src"
}

# ─── Spanish MASTER version (docs/radarlib_ES.md → docs/radarlib_ES.pdf) ──────
generate_es_master() {
    local es_src="$SCRIPT_DIR/radarlib_ES.md"
    local out_pdf="$SCRIPT_DIR/radarlib_ES.pdf"
    local out_html="$SCRIPT_DIR/radarlib_ES.html"

    echo "[ ES ] Generando documentación en español (MÁSTER)..."
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
    EN)
        generate_en_master
        ;;
    ES)
        generate_es_master
        ;;
    master)
        generate_en_master
        echo ""
        generate_es_master
        ;;
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
        generate_en_master
        echo ""
        generate_es_master
        echo ""
        echo "────────────────────────────────────────────────────"
        echo "Versiones Legacy (para compatibilidad):"
        echo "────────────────────────────────────────────────────"
        echo ""
        generate_en
        echo ""
        generate_es_single
        echo ""
        generate_es_multi
        ;;
    *)
        echo "Argumento no reconocido: '$LANG_ARG'"
        echo ""
        echo "Uso: $0 [EN|ES|en|es|es-single|master|all]"
        echo ""
        echo "MÁSTER (Recomendado - Consolidado):"
        echo "  EN        — Documentación en inglés (docs/radarlib_EN.md)"
        echo "  ES        — Documentación en español (docs/radarlib_ES.md)"
        echo "  master    — Ambas versiones MÁSTER (EN + ES) [DEFECTO]"
        echo ""
        echo "LEGACY (Compatibilidad):"
        echo "  en        — Documentación en inglés (docs/README.md)"
        echo "  es-single — Documentación en español, archivo único (docs/README.es.md)"
        echo "  es        — Documentación en español, secciones separadas (docs/es/*.md)"
        echo ""
        echo "Opciones:"
        echo "  all       — Todas las versiones (MÁSTER + LEGACY)"
        exit 1
        ;;
esac

echo ""
echo "Proceso completado."
