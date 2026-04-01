"""
Radar COG Colormap Viewer — Streamlit App

Load a raw-float COG produced by radarlib and interactively switch colormaps,
adjust vmin/vmax, and browse overview levels.
"""

import io
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import rasterio
import streamlit as st
from PIL import Image

import radarlib  # noqa: F401  – registers custom colormaps
from radarlib.radar_grid import read_cog_metadata, read_cog_tile_as_rgba

# ---------------------------------------------------------------------------
# Make radarlib importable when it is not installed as a package
# ---------------------------------------------------------------------------
# _SRC_DIR = str(Path(__file__).resolve().parent.parent / "src")
# if _SRC_DIR not in sys.path:
#     sys.path.insert(0, _SRC_DIR)


print(radarlib.config)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Radar-specific colormaps registered by radarlib (grc_* prefix)
_RADARLIB_CMAPS = sorted(name for name in plt.colormaps() if name.startswith("grc_"))

# A curated set of standard matplotlib colormaps useful for radar data
_STANDARD_CMAPS = [
    "viridis",
    "plasma",
    "inferno",
    "magma",
    "cividis",
    "hot",
    "jet",
    "turbo",
    "coolwarm",
    "RdYlBu_r",
    "Spectral_r",
    "rainbow",
    "gnuplot2",
    "cubehelix",
]

ALL_CMAPS = _RADARLIB_CMAPS + _STANDARD_CMAPS


def _rgba_to_png_bytes(rgba: np.ndarray) -> bytes:
    """Convert an RGBA numpy array to PNG bytes for download."""
    img = Image.fromarray(rgba, mode="RGBA")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _make_colorbar(cmap_name: str, vmin: float, vmax: float) -> bytes:
    """Render a horizontal colorbar as PNG bytes."""
    fig, ax = plt.subplots(figsize=(6, 0.4))
    gradient = np.linspace(0, 1, 256).reshape(1, -1)
    ax.imshow(gradient, aspect="auto", cmap=cmap_name, extent=(vmin, vmax, 0.0, 1.0))
    ax.set_yticks([])
    ax.set_xlabel("")
    ax.tick_params(labelsize=7)
    fig.tight_layout(pad=0.3)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Radar COG Colormap Viewer",
    page_icon="🌧️",
    layout="wide",
)

st.title("🌧️ Radar COG Colormap Viewer")
st.markdown(
    "Load a **raw-float COG** created with `radarlib` and interactively "
    "change its colormap, value range and overview level."
)

# ---------------------------------------------------------------------------
# Sidebar — file selection
# ---------------------------------------------------------------------------
st.sidebar.header("COG File")

upload_mode = st.sidebar.radio(
    "Source",
    ["Local path", "Upload file"],
    horizontal=True,
)

cog_path: "Path | None" = None

if upload_mode == "Upload file":
    uploaded = st.sidebar.file_uploader("Upload a .cog / .tif file", type=["cog", "tif", "tiff"])
    if uploaded is not None:
        # Write to a temporary file so rasterio can open it
        import tempfile

        tmp = Path(tempfile.mktemp(suffix=".cog"))
        tmp.write_bytes(uploaded.read())
        cog_path = tmp
else:
    default_path = str(
        Path(__file__).resolve().parent.parent / "outputs" / "colormaps" / "real_data_reflectivity_raw.cog"
    )
    path_str = st.sidebar.text_input("COG file path", value=default_path)
    if path_str:
        p = Path(path_str)
        if p.is_file():
            cog_path = p
        else:
            st.sidebar.error(f"File not found: {path_str}")

# ---------------------------------------------------------------------------
# Early exit if no file
# ---------------------------------------------------------------------------
if cog_path is None:
    st.info("👈 Select or upload a COG file from the sidebar to get started.")
    st.stop()

# ---------------------------------------------------------------------------
# Read metadata
# ---------------------------------------------------------------------------
meta = read_cog_metadata(cog_path)

st.sidebar.markdown("---")
st.sidebar.subheader("File metadata")
st.sidebar.json({k: (str(v) if v is not None else "—") for k, v in meta.items()})

file_vmin = meta.get("vmin") if meta.get("vmin") is not None else 0.0
file_vmax = meta.get("vmax") if meta.get("vmax") is not None else 70.0
file_cmap = meta.get("cmap") or "viridis"
data_type = meta.get("data_type") or "unknown"

if data_type != "raw_float":
    st.warning(
        f'This file has `data_type = "{data_type}"`. '
        "Colormap switching works best with raw-float COGs created by "
        "`create_raw_cog()`. The image will still be displayed but "
        "colormap changes may not apply."
    )

# ---------------------------------------------------------------------------
# Sidebar — colormap controls
# ---------------------------------------------------------------------------
st.sidebar.markdown("---")
st.sidebar.subheader("Colormap")

# Default selection index
default_idx = 0
if file_cmap in ALL_CMAPS:
    default_idx = ALL_CMAPS.index(file_cmap)

selected_cmap = st.sidebar.selectbox(
    "Colormap",
    ALL_CMAPS,
    index=default_idx,
    help="Choose any matplotlib or radarlib colormap",
)

col_vmin, col_vmax = st.sidebar.columns(2)
vmin = col_vmin.number_input("vmin", value=float(file_vmin), step=1.0, format="%.1f")
vmax = col_vmax.number_input("vmax", value=float(file_vmax), step=1.0, format="%.1f")

if vmin >= vmax:
    st.sidebar.error("vmin must be less than vmax")
    st.stop()

with rasterio.open(cog_path) as src:
    n_overviews = len(src.overviews(1))

overview_level = st.sidebar.slider(
    "Overview level",
    min_value=0,
    max_value=max(n_overviews, 1),
    value=0,
    help="0 = full resolution, higher = more downsampled",
)

# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------
st.markdown("---")

try:
    rgba = read_cog_tile_as_rgba(
        cog_path,
        cmap=selected_cmap,
        vmin=vmin,
        vmax=vmax,
        overview_level=overview_level,
    )
except Exception as exc:
    st.error(f"Error reading COG: {exc}")
    st.stop()

# Layout: image + colorbar
col_img, col_info = st.columns([3, 1])

with col_img:
    st.image(
        rgba,
        caption=f"{selected_cmap}  |  vmin={vmin}  vmax={vmax}  |  overview={overview_level}",
        use_container_width=True,
    )

with col_info:
    st.markdown(f"**Image size:** {rgba.shape[1]} × {rgba.shape[0]} px")
    st.markdown(f"**Colormap:** `{selected_cmap}`")
    st.markdown(f"**Range:** [{vmin}, {vmax}]")
    st.markdown(f"**Overview level:** {overview_level}")
    st.markdown(f"**Data type:** {data_type}")

    # Colorbar preview
    st.markdown("**Colorbar:**")
    cbar_bytes = _make_colorbar(selected_cmap, vmin, vmax)
    st.image(cbar_bytes, use_container_width=True)

    # Download button
    png_bytes = _rgba_to_png_bytes(rgba)
    st.download_button(
        label="📥 Download PNG",
        data=png_bytes,
        file_name=f"radar_{selected_cmap}_v{vmin}-{vmax}.png",
        mime="image/png",
    )

# ---------------------------------------------------------------------------
# Side-by-side comparison
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("Side-by-side comparison")
st.markdown("Pick two colormaps to compare them on the same data.")

comp_col1, comp_col2 = st.columns(2)

with comp_col1:
    cmap_a = st.selectbox("Left colormap", ALL_CMAPS, index=ALL_CMAPS.index(selected_cmap), key="cmp_a")
with comp_col2:
    second_default = ALL_CMAPS.index("jet") if "jet" in ALL_CMAPS else 0
    cmap_b = st.selectbox("Right colormap", ALL_CMAPS, index=second_default, key="cmp_b")

img_col1, img_col2 = st.columns(2)

with img_col1:
    rgba_a = read_cog_tile_as_rgba(cog_path, cmap=cmap_a, vmin=vmin, vmax=vmax, overview_level=overview_level)
    st.image(rgba_a, caption=cmap_a, use_container_width=True)
    st.image(_make_colorbar(cmap_a, vmin, vmax), use_container_width=True)

with img_col2:
    rgba_b = read_cog_tile_as_rgba(cog_path, cmap=cmap_b, vmin=vmin, vmax=vmax, overview_level=overview_level)
    st.image(rgba_b, caption=cmap_b, use_container_width=True)
    st.image(_make_colorbar(cmap_b, vmin, vmax), use_container_width=True)
