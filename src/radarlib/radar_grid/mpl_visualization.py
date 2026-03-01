"""
Quick visualization for notebook testing.
Save to: /workspaces/radar-processor/src/radar_grid/visualization.py
"""

import logging
from typing import Dict, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np

logger = logging.getLogger(__name__)

# Default color maps and ranges for common radar fields
FIELD_CONFIGS = {
    "DBZH": {
        "cmap": "pyart_NWSRef",
        "vmin": -10,
        "vmax": 70,
        "label": "Reflectivity (dBZ)",
    },
    "DBZV": {
        "cmap": "pyart_NWSRef",
        "vmin": -10,
        "vmax": 70,
        "label": "Reflectivity V (dBZ)",
    },
    "ZDR": {
        "cmap": "pyart_RefDiff",
        "vmin": -2,
        "vmax": 8,
        "label": "Differential Reflectivity (dB)",
    },
    "KDP": {
        "cmap": "pyart_Theodore16",
        "vmin": -1,
        "vmax": 5,
        "label": "Specific Differential Phase (°/km)",
    },
    "RHOHV": {
        "cmap": "pyart_RefDiff",
        "vmin": 0.7,
        "vmax": 1.0,
        "label": "Correlation Coefficient",
    },
    "VRAD": {
        "cmap": "pyart_BuDRd18",
        "vmin": -30,
        "vmax": 30,
        "label": "Radial Velocity (m/s)",
    },
}

# Fallback colormaps if pyart colormaps not available
FALLBACK_CMAPS = {
    "pyart_NWSRef": "jet",
    "pyart_RefDiff": "RdBu_r",
    "pyart_Theodore16": "viridis",
    "pyart_BuDRd18": "RdBu_r",
}


def get_cmap(cmap_name: str):
    """Get colormap, falling back to matplotlib default if pyart not available."""
    try:
        return plt.get_cmap(cmap_name)
    except ValueError:
        fallback = FALLBACK_CMAPS.get(cmap_name, "viridis")
        return plt.get_cmap(fallback)


def plot_grid_slice(
    grid: np.ndarray,
    z_index: int = 0,
    field_name: Optional[str] = None,
    title: Optional[str] = None,
    cmap: Optional[str] = None,
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    figsize: Tuple[int, int] = (10, 8),
    colorbar: bool = True,
    ax: Optional[plt.Axes] = None,
    show: bool = True,
) -> Optional[plt.Figure]:
    """
    Plot a horizontal slice of the gridded data.

    Parameters
    ----------
    grid : np.ndarray
        3D gridded data, shape (nz, ny, nx)
    z_index : int
        Index of the z-level to plot (default: 0)
    field_name : str, optional
        Field name for automatic color scaling (e.g., 'DBZH', 'ZDR')
    title : str, optional
        Plot title. If None, auto-generated from field_name and z_index
    cmap : str, optional
        Colormap name. If None, uses field default or 'viridis'
    vmin, vmax : float, optional
        Color scale limits. If None, uses field defaults or data range
    figsize : tuple
        Figure size (width, height) in inches
    colorbar : bool
        Whether to show colorbar
    ax : matplotlib.axes.Axes, optional
        Axes to plot on. If None, creates new figure
    show : bool
        If True, displays the plot. If False, returns figure without displaying.

    Returns
    -------
    matplotlib.figure.Figure or None
        Returns figure only if show=False
    """
    # Get field configuration
    config = FIELD_CONFIGS.get(field_name, {})

    # Set defaults from config or fallbacks
    if cmap is None:
        cmap = config.get("cmap", "viridis")
    if vmin is None:
        vmin = config.get("vmin", np.nanmin(grid))
    if vmax is None:
        vmax = config.get("vmax", np.nanmax(grid))

    cmap_obj = get_cmap(cmap)

    # Extract slice
    data_slice = grid[z_index, :, :]

    # Create figure if needed
    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
        created_fig = True
    else:
        fig = ax.figure

    # Plot
    im = ax.imshow(data_slice, origin="lower", cmap=cmap_obj, vmin=vmin, vmax=vmax, aspect="equal")

    # Colorbar
    if colorbar:
        label = config.get("label", field_name or "")
        plt.colorbar(im, ax=ax, label=label, shrink=0.8)

    # Title
    if title is None:
        field_str = field_name or "Data"
        title = f"{field_str} - Z level {z_index}"
    ax.set_title(title)

    ax.set_xlabel("X index")
    ax.set_ylabel("Y index")

    if created_fig:
        plt.tight_layout()

    if show:
        plt.show()
        return None
    else:
        return fig


def plot_grid_multi_level(
    grid: np.ndarray,
    field_name: Optional[str] = None,
    z_indices: Optional[list] = None,
    ncols: int = 3,
    figsize_per_plot: Tuple[float, float] = (4, 3.5),
    show: bool = True,
    **kwargs,
) -> Optional[plt.Figure]:
    """
    Plot multiple z-levels in a grid layout.

    Parameters
    ----------
    grid : np.ndarray
        3D gridded data, shape (nz, ny, nx)
    field_name : str, optional
        Field name for automatic color scaling
    z_indices : list, optional
        List of z-indices to plot. If None, plots all levels
    ncols : int
        Number of columns in the subplot grid
    figsize_per_plot : tuple
        Size of each subplot
    show : bool
        If True, displays the plot. If False, returns figure without displaying.
    **kwargs
        Additional arguments passed to imshow

    Returns
    -------
    matplotlib.figure.Figure or None
        Returns figure only if show=False
    """
    nz = grid.shape[0]

    if z_indices is None:
        z_indices = list(range(nz))

    n_plots = len(z_indices)
    nrows = (n_plots + ncols - 1) // ncols

    # Add extra width for colorbar
    figsize = (figsize_per_plot[0] * ncols + 1, figsize_per_plot[1] * nrows)
    fig, axes = plt.subplots(nrows, ncols, figsize=figsize)

    if n_plots == 1:
        axes = np.array([axes])
    axes = axes.flatten()

    # Get consistent color scale
    config = FIELD_CONFIGS.get(field_name, {})
    vmin = kwargs.pop("vmin", config.get("vmin", np.nanmin(grid)))
    vmax = kwargs.pop("vmax", config.get("vmax", np.nanmax(grid)))
    cmap = kwargs.pop("cmap", config.get("cmap", "viridis"))
    cmap_obj = get_cmap(cmap)

    for i, z_idx in enumerate(z_indices):
        data_slice = grid[z_idx, :, :]
        im = axes[i].imshow(data_slice, origin="lower", cmap=cmap_obj, vmin=vmin, vmax=vmax, aspect="equal", **kwargs)
        axes[i].set_title(f"Z level {z_idx}")
        axes[i].set_xlabel("X index")
        axes[i].set_ylabel("Y index")

    # Hide empty subplots
    for i in range(n_plots, len(axes)):
        axes[i].set_visible(False)

    # Add colorbar on the right side of the figure
    label = config.get("label", field_name or "")
    fig.subplots_adjust(right=0.88)
    cbar_ax = fig.add_axes([0.90, 0.15, 0.02, 0.7])  # [left, bottom, width, height]
    fig.colorbar(im, cax=cbar_ax, label=label)

    if field_name:
        fig.suptitle(f"{field_name} - All Z levels", fontsize=14)

    if show:
        plt.show()
        return None
    else:
        return fig


def plot_all_fields(
    grids: Dict[str, np.ndarray],
    z_index: int = 0,
    ncols: int = 2,
    figsize_per_plot: Tuple[float, float] = (5, 4),
    show: bool = True,
) -> Optional[plt.Figure]:
    """
    Plot all fields at a given z-level.

    Parameters
    ----------
    grids : dict
        Dictionary of {field_name: grid_data}
    z_index : int
        Z-level to plot
    ncols : int
        Number of columns
    figsize_per_plot : tuple
        Size of each subplot
    show : bool
        If True, displays the plot. If False, returns figure without displaying.

    Returns
    -------
    matplotlib.figure.Figure or None
        Returns figure only if show=False
    """
    field_names = list(grids.keys())
    n_plots = len(field_names)
    nrows = (n_plots + ncols - 1) // ncols

    figsize = (figsize_per_plot[0] * ncols, figsize_per_plot[1] * nrows)
    fig, axes = plt.subplots(nrows, ncols, figsize=figsize)

    if n_plots == 1:
        axes = np.array([axes])
    axes = axes.flatten()

    for i, field_name in enumerate(field_names):
        grid = grids[field_name]
        config = FIELD_CONFIGS.get(field_name, {})

        cmap = config.get("cmap", "viridis")
        vmin = config.get("vmin", np.nanmin(grid))
        vmax = config.get("vmax", np.nanmax(grid))
        label = config.get("label", field_name)

        cmap_obj = get_cmap(cmap)
        data_slice = grid[z_index, :, :]

        im = axes[i].imshow(data_slice, origin="lower", cmap=cmap_obj, vmin=vmin, vmax=vmax, aspect="equal")
        axes[i].set_title(field_name)
        axes[i].set_xlabel("X index")
        axes[i].set_ylabel("Y index")
        fig.colorbar(im, ax=axes[i], label=label, shrink=0.8)

    # Hide empty subplots
    for i in range(n_plots, len(axes)):
        axes[i].set_visible(False)

    fig.suptitle(f"All Fields - Z level {z_index}", fontsize=14, y=1.02)
    plt.tight_layout()

    if show:
        plt.show()
        return None
    else:
        return fig


def plot_vertical_cross_section(
    grid: np.ndarray,
    y_index: Optional[int] = None,
    x_index: Optional[int] = None,
    field_name: Optional[str] = None,
    title: Optional[str] = None,
    figsize: Tuple[int, int] = (12, 5),
    show: bool = True,
    **kwargs,
) -> Optional[plt.Figure]:
    """
    Plot a vertical cross-section through the grid.

    Parameters
    ----------
    grid : np.ndarray
        3D gridded data, shape (nz, ny, nx)
    y_index : int, optional
        Y-index for cross-section (plots X-Z plane). Default: center
    x_index : int, optional
        X-index for cross-section (plots Y-Z plane). If both given, y_index used.
    field_name : str, optional
        Field name for color scaling
    title : str, optional
        Plot title
    figsize : tuple
        Figure size
    show : bool
        If True, displays the plot. If False, returns figure without displaying.
    **kwargs
        Additional arguments passed to imshow

    Returns
    -------
    matplotlib.figure.Figure or None
        Returns figure only if show=False
    """
    nz, ny, nx = grid.shape

    # Determine which cross-section to take
    if y_index is not None:
        data_slice = grid[:, y_index, :]  # X-Z plane
        xlabel = "X index"
        section_type = f"Y={y_index}"
    elif x_index is not None:
        data_slice = grid[:, :, x_index]  # Y-Z plane
        xlabel = "Y index"
        section_type = f"X={x_index}"
    else:
        # Default to center Y
        y_index = ny // 2
        data_slice = grid[:, y_index, :]
        xlabel = "X index"
        section_type = f"Y={y_index} (center)"

    # Get field configuration
    config = FIELD_CONFIGS.get(field_name, {})
    cmap = kwargs.pop("cmap", config.get("cmap", "viridis"))
    vmin = kwargs.pop("vmin", config.get("vmin", np.nanmin(grid)))
    vmax = kwargs.pop("vmax", config.get("vmax", np.nanmax(grid)))

    cmap_obj = get_cmap(cmap)

    fig, ax = plt.subplots(figsize=figsize)

    im = ax.imshow(data_slice, origin="lower", cmap=cmap_obj, vmin=vmin, vmax=vmax, aspect="auto", **kwargs)

    label = config.get("label", field_name or "")
    plt.colorbar(im, ax=ax, label=label)

    if title is None:
        field_str = field_name or "Data"
        title = f"{field_str} - Vertical Cross-Section ({section_type})"
    ax.set_title(title)

    ax.set_xlabel(xlabel)
    ax.set_ylabel("Z index")

    plt.tight_layout()

    if show:
        plt.show()
        return None
    else:
        return fig
