# Radar COG Colormap Viewer

A simple Streamlit app for loading radar COG files (created with **radarlib**) and
interactively applying different colormaps.

## Setup

```bash
cd streamlit_app
pip install -r requirements.txt
```

> **Note:** `radarlib` must be importable. If it is not installed, the app
> automatically adds `../src` to `sys.path`.

## Run

```bash
streamlit run app.py
```

## Features

- Load any raw-float COG created by `radarlib.radar_grid.create_raw_cog`
- Switch colormaps on-the-fly (matplotlib built-ins + radarlib `grc_*` customs)
- Adjust **vmin / vmax** range interactively
- View different overview (zoom) levels
- Download the rendered tile as PNG
