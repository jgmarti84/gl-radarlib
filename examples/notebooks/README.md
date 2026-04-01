# Jupyter Notebooks

This directory contains Jupyter notebooks that demonstrate features of the `radarlib`
library using synthetic data (no real radar files required).

## colormap_modification.ipynb

Demonstrates the colormap modification feature end-to-end:

- **`create_raw_cog`** — save raw float scientific values as a COG with colormap metadata
- **`read_cog_tile_as_rgba`** — render any COG tile as an RGBA array with any colormap applied
  on-the-fly (great for API tile servers)
- **`remap_cog_colormap`** — produce a new RGBA COG with a different colormap without touching
  the original file
- **`read_cog_metadata`** — inspect the colormap/vmin/vmax metadata embedded in a COG

### Running the Notebook

```bash
pip install jupyter
jupyter notebook colormap_modification.ipynb
```

If the `radarlib` package is not installed, add the `src` directory to the Python path:

```python
import sys
sys.path.insert(0, "../../src")
import radarlib
```
