"""
ppi_ring_fix_example.py
=======================
Demonstrates and verifies the fix for the ring-pattern artefact that appeared
on low-elevation PPI images extracted from a 3D Cartesian grid.

ROOT CAUSE
----------
When the below-beam mask is active during geometry computation, z-levels below
the lowest radar beam have no gate contributions and are stored as NaN in the
3D grid.  The old ``constant_elevation_ppi`` used a plain linear interpolation:

    result = weight_low * val_low + weight_high * val_high

If ``val_low`` (the bracketing level just below the target beam height) is NaN,
floating-point arithmetic propagates the NaN to the result, creating a concentric
ring of missing data at the range where the beam height crosses a z-level boundary.

THE FIX
-------
The new implementation (ported from ``radar_processing/product_collapse.py``) is
NaN-aware.  It classifies every pixel into one of four cases before doing any
arithmetic:
    * both levels valid   → normal linear interpolation
    * only z_low valid    → use z_low directly
    * only z_high valid   → use z_high directly  ← this case eliminates the rings
    * neither valid       → NaN (genuinely unobserved)

HOW THE EXAMPLE WORKS
---------------------
A synthetic 3D grid is constructed that faithfully reproduces the below-beam
masking pattern:
  - Lower z-levels (simulating below-beam voxels) are NaN inside a ring band.
  - Higher z-levels have valid reflectivity values everywhere.

Both the old (naive) and new (NaN-aware) interpolation behaviours are computed
side-by-side and compared so the difference is measurable without needing a
real radar file.

Optionally, if a real radar file and pre-computed geometry are available, the
example also runs both paths on real data and plots the results.
"""

import os

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

matplotlib.use("Agg")  # headless-safe backend

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _old_linear_ppi(grid, z_min, z_max, nz, ny, nx, target_z):
    """Replicates the PRE-FIX linear interpolation (NaN-propagating)."""
    z_step = (z_max - z_min) / (nz - 1) if nz > 1 else 1.0
    z_frac = (target_z - z_min) / z_step
    z_low = np.floor(z_frac).astype(int)
    z_high = z_low + 1
    weight_high = z_frac - z_low
    weight_low = 1.0 - weight_high
    below_grid = target_z < z_min
    above_grid = target_z > z_max
    z_low_safe = np.clip(z_low, 0, nz - 1)
    z_high_safe = np.clip(z_high, 0, nz - 1)
    y_idx, x_idx = np.meshgrid(np.arange(ny), np.arange(nx), indexing="ij")
    val_low = grid[z_low_safe, y_idx, x_idx]
    val_high = grid[z_high_safe, y_idx, x_idx]
    # Plain multiplication — NaN propagates
    result = weight_low * val_low + weight_high * val_high
    result[below_grid] = np.nan
    result[above_grid] = np.nan
    return result.astype("float32")


def _new_linear_ppi(grid, z_min, z_max, nz, ny, nx, target_z):
    """Replicates the POST-FIX NaN-aware linear interpolation."""
    z_step = (z_max - z_min) / (nz - 1) if nz > 1 else 1.0
    z_frac = (target_z - z_min) / z_step
    z_low = np.floor(z_frac).astype(int)
    z_high = z_low + 1
    weight_high = z_frac - z_low
    weight_low = 1.0 - weight_high
    below_grid = target_z < z_min
    above_grid = target_z > z_max
    z_low_safe = np.clip(z_low, 0, nz - 1)
    z_high_safe = np.clip(z_high, 0, nz - 1)
    y_idx, x_idx = np.meshgrid(np.arange(ny), np.arange(nx), indexing="ij")
    val_low = grid[z_low_safe, y_idx, x_idx]
    val_high = grid[z_high_safe, y_idx, x_idx]
    # NaN-aware four-case logic
    nan_low = np.isnan(val_low)
    nan_high = np.isnan(val_high)
    both_valid = ~nan_low & ~nan_high
    only_low = ~nan_low & nan_high
    only_high = nan_low & ~nan_high
    neither = nan_low & nan_high
    result_data = np.zeros((ny, nx), dtype="float32")
    result_data[both_valid] = (
        weight_low[both_valid] * val_low[both_valid] + weight_high[both_valid] * val_high[both_valid]
    )
    result_data[only_low] = val_low[only_low]
    result_data[only_high] = val_high[only_high]
    result = np.where(neither, np.nan, result_data).astype("float32")
    result[below_grid] = np.nan
    result[above_grid] = np.nan
    return result


# ---------------------------------------------------------------------------
# PART 1 – Synthetic test
# ---------------------------------------------------------------------------


def run_synthetic_test(output_dir: str = "outputs/radar_grid/ring_fix"):
    """
    Build a synthetic 3D grid that mimics the below-beam masking pattern and
    compare old vs. new PPI extraction side by side.

    WHY RINGS APPEAR IN THE OLD CODE
    ---------------------------------
    The below-beam mask leaves NaN in level iz wherever:
        z_coords[iz] < beam_height(r, lowest_elev)

    For a PPI at the same lowest elevation, target_z ≈ beam_height(r).
    When that target_z sits just ABOVE a z-level boundary, the bracketing
    pair is:
        z_low  = that boundary level → NaN (beam just rose past it)
        z_high = the next level up   → valid data

    Old code: weight_low * NaN + weight_high * val = NaN  → ring gap
    New code: z_low is NaN, z_high is valid → fall back to z_high → filled

    To make the ring visually clear in the comparison image we need most
    pixels to have valid data in the old image too (not all-NaN).  This is
    achieved by leaving z_level[0] (the surface slice at z=0 m) intact —
    at short ranges the beam hasn't risen above z_step yet, so z_low=0 is
    valid and the old code produces correct data there.  The NaN rings then
    appear as distinct concentric gaps at the ranges where the beam height
    crosses each z-level boundary (one ring per z-step crossing).
    """
    print("=" * 60)
    print("PART 1 – SYNTHETIC BELOW-BEAM RING TEST")
    print("=" * 60)

    # Grid dimensions — coarse z resolution maximises ring width
    nz, ny, nx = 15, 200, 200
    z_step = 500.0  # metres between levels (→ rings every ~57 km)
    z_min = 0.0
    z_max = z_min + z_step * (nz - 1)  # 7000 m
    xy_min, xy_max = -200_000.0, 200_000.0  # ±200 km

    z_coords = np.linspace(z_min, z_max, nz)
    y_coords = np.linspace(xy_min, xy_max, ny)
    x_coords = np.linspace(xy_min, xy_max, nx)

    # --- Build synthetic 3D reflectivity field ---
    # Realistic-looking radar reflectivity with a convective core near the
    # centre and a stratiform ring at medium range.
    yy, xx = np.meshgrid(y_coords, x_coords, indexing="ij")
    horiz_dist = np.sqrt(xx**2 + yy**2)

    # Convective core + stratiform gradient
    core = 50.0 * np.exp(-((horiz_dist / 40_000.0) ** 2))
    strat = 35.0 * np.exp(-(((horiz_dist - 100_000.0) / 40_000.0) ** 2))
    base_dbzh = np.clip(core + strat, 0.0, 60.0).astype("float32")

    # Replicate across all z-levels (values decrease slightly with altitude)
    altitude_decay = np.exp(-z_coords / 8_000.0).astype("float32")  # shape (nz,)
    grid = (base_dbzh[np.newaxis, :, :] * altitude_decay[:, np.newaxis, np.newaxis]).copy()

    # --- Simulate below-beam mask (4/3 Earth-radius model) ---
    # The lowest radar beam at 0.5° increases in height with range.
    # Levels *above* the surface (iz >= 1) are masked wherever the beam
    # height has not yet reached them — i.e. where gz < beam_h(r).
    # Level iz=0 (z=0 m) is left intact: at short range the beam is close
    # to the surface and surface-level data is always present in a real scan.
    elev_rad = np.radians(0.5)
    ke_re = (4.0 / 3.0) * 6.371e6
    slant = horiz_dist / np.maximum(np.cos(elev_rad), 0.01)
    beam_h = np.sqrt(slant**2 + ke_re**2 + 2 * slant * ke_re * np.sin(elev_rad)) - ke_re

    for iz in range(1, nz):  # skip iz=0 (surface level)
        gz = z_coords[iz]
        grid[iz, gz < beam_h] = np.nan

    valid_per_level = np.sum(~np.isnan(grid), axis=(1, 2))
    print(f"\nGrid shape     : {grid.shape}")
    print(f"z_coords       : {z_coords[0]:.0f} – {z_coords[-1]:.0f} m  (step={z_step:.0f} m)")
    print(f"Valid voxels / level: {valid_per_level.tolist()}")
    print(f"NaN voxels (below-beam): {np.sum(np.isnan(grid)):,} / {grid.size:,}")
    print("\nExpected ring radii (where beam crosses each z-level boundary):")
    for iz in range(1, min(6, nz)):
        gz = z_coords[iz]
        # Approximate range where beam_h ≈ gz  (flat-Earth approximation for clarity)
        r_ring_km = gz / np.tan(elev_rad) / 1_000.0
        print(f"  z={gz:.0f} m  →  r ≈ {r_ring_km:.0f} km")

    # --- PPI extraction at exactly 0.5° ---
    # target_z follows the beam surface, so at each range band where the beam
    # has just crossed a z-level boundary the following bracket occurs:
    #   z_low  = that boundary level → NaN   (beam rose past it)
    #   z_high = next level up       → valid
    # The old code returns NaN there; the new code falls back to z_high.
    target_z = beam_h

    ppi_old = _old_linear_ppi(grid, z_min, z_max, nz, ny, nx, target_z)
    ppi_new = _new_linear_ppi(grid, z_min, z_max, nz, ny, nx, target_z)

    nan_old = int(np.sum(np.isnan(ppi_old)))
    nan_new = int(np.sum(np.isnan(ppi_new)))
    recovered = nan_old - nan_new

    print("\nPPI extraction at 0.5° elevation:")
    print(f"  Old (NaN-propagating) : {nan_old:,} NaN pixels")
    print(f"  New (NaN-aware)       : {nan_new:,} NaN pixels")
    print(f"  Pixels recovered      : {recovered:,}  ({100*recovered/ppi_old.size:.1f}% of grid)")

    assert recovered > 0, "FIX FAILED: new method should recover pixels that the old method lost"
    print("\n  ✓  Fix verified: NaN-aware interpolation eliminates the ring artefacts.")

    # --- Save comparison figure ---
    os.makedirs(output_dir, exist_ok=True)
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))

    vmin, vmax = 0, 55
    kw = dict(vmin=vmin, vmax=vmax, cmap="NWSRef", origin="lower")
    extent = [xy_min / 1e3, xy_max / 1e3, xy_min / 1e3, xy_max / 1e3]

    im0 = axes[0].imshow(ppi_old, extent=extent, **kw)
    axes[0].set_title("Old: NaN-propagating linear\n(concentric ring gaps)")
    axes[0].set_xlabel("X (km)")
    axes[0].set_ylabel("Y (km)")
    plt.colorbar(im0, ax=axes[0], label="dBZ")

    im1 = axes[1].imshow(ppi_new, extent=extent, **kw)
    axes[1].set_title("New: NaN-aware linear\n(rings eliminated)")
    axes[1].set_xlabel("X (km)")
    plt.colorbar(im1, ax=axes[1], label="dBZ")

    # Difference: NaN in old but valid in new → ring pixels recovered
    ring_mask = np.isnan(ppi_old) & ~np.isnan(ppi_new)
    diff_display = np.zeros((ny, nx), dtype="float32")
    diff_display[ring_mask] = 1.0  # recovered pixels in red
    diff_display[np.isnan(ppi_new)] = -1.0  # still NaN (genuinely unobserved) in blue

    im2 = axes[2].imshow(diff_display, extent=extent, vmin=-1, vmax=1, cmap="RdBu_r", origin="lower")
    axes[2].set_title("Coverage map\n(red = ring pixels recovered by fix)")
    axes[2].set_xlabel("X (km)")
    plt.colorbar(im2, ax=axes[2], label="+1 recovered  /  −1 unobserved")

    fig.suptitle(
        f"PPI Ring Fix – Synthetic Test  |  {recovered:,} pixels recovered "
        f"({100*recovered/ppi_old.size:.1f}% of grid)",
        fontsize=12,
    )
    fig.tight_layout()
    out_path = os.path.join(output_dir, "synthetic_ring_fix_comparison.png")
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"\n  Figure saved → {out_path}")


# ---------------------------------------------------------------------------
# PART 2 – Real data test (optional, skipped if files not present)
# ---------------------------------------------------------------------------

RADAR_FILE = "./app/data/radares/RMA1/netcdf/RMA1_0315_01_20260326T221324Z.nc"
GEOMETRY_FILE = (
    "./app/data/geometries/" "RMA1_0315_01_RES1000x600_TOA15000_HF1p0000_MR900_MN1_NB1p30_BSP1p10_nearest_geometry.npz"
)


def run_real_data_test(output_dir: str = "outputs/radar_grid/ring_fix"):
    """
    If a real radar file and pre-computed geometry are available, extract a
    low-elevation PPI with both the old and new methods and plot the results.
    """
    if not os.path.isfile(RADAR_FILE) or not os.path.isfile(GEOMETRY_FILE):
        print("\nPART 2 – REAL DATA TEST")
        print("  Skipped: radar file or geometry file not found.")
        print(f"    Radar    : {RADAR_FILE}")
        print(f"    Geometry : {GEOMETRY_FILE}")
        return

    print("\n" + "=" * 60)
    print("PART 2 – REAL DATA TEST")
    print("=" * 60)

    import pyart

    from radarlib.radar_grid import apply_geometry, get_field_data, load_geometry
    from radarlib.radar_grid.products import compute_beam_height, constant_elevation_ppi

    radar = pyart.io.read(RADAR_FILE)
    geometry = load_geometry(GEOMETRY_FILE)
    nz, ny, nx = geometry.grid_shape
    z_min, z_max = geometry.grid_limits[0]
    y_min, y_max = geometry.grid_limits[1]
    x_min, x_max = geometry.grid_limits[2]

    dbzh_data = get_field_data(radar, "DBZH")
    grid_dbzh = apply_geometry(geometry, dbzh_data)
    print(f"3D grid shape : {grid_dbzh.shape}")
    print(f"NaN voxels    : {np.sum(np.isnan(grid_dbzh)):,} / {grid_dbzh.size:,}")

    elevation = 0.5  # degrees

    # New (fixed) path — calls constant_elevation_ppi which now uses NaN-aware logic
    ppi_new = constant_elevation_ppi(grid_dbzh, geometry, elevation_angle=elevation, interpolation="linear")

    # Old path — manually replicate the pre-fix kernel
    y_coords = np.linspace(y_min, y_max, ny, dtype="float32")
    x_coords = np.linspace(x_min, x_max, nx, dtype="float32")
    yy, xx = np.meshgrid(y_coords, x_coords, indexing="ij")
    target_z = compute_beam_height(np.sqrt(xx**2 + yy**2), elevation, radar_altitude=0.0)
    ppi_old = _old_linear_ppi(grid_dbzh, z_min, z_max, nz, ny, nx, target_z)

    nan_old = np.sum(np.isnan(ppi_old))
    nan_new = np.sum(np.isnan(ppi_new))
    print(f"\nPPI at {elevation}° elevation:")
    print(f"  Old NaN pixels : {nan_old:,}")
    print(f"  New NaN pixels : {nan_new:,}")
    print(f"  Pixels recovered : {nan_old - nan_new:,}")

    os.makedirs(output_dir, exist_ok=True)
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    kw = dict(vmin=-10, vmax=55, cmap="NWSRef", origin="lower")
    axes[0].imshow(ppi_old, **kw)
    axes[0].set_title(f"Old: NaN-propagating  ({nan_old:,} NaN px)")
    axes[1].imshow(ppi_new, **kw)
    axes[1].set_title(f"New: NaN-aware  ({nan_new:,} NaN px)")
    for ax in axes:
        ax.set_xlabel("X index")
        ax.set_ylabel("Y index")
    fig.suptitle(f"Real data – PPI at {elevation}°", fontsize=13)
    fig.tight_layout()
    out_path = os.path.join(output_dir, f"real_data_ppi_{elevation}deg_comparison.png")
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  Figure saved → {out_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    OUT = "outputs/radar_grid/ring_fix"
    run_synthetic_test(output_dir=OUT)
    run_real_data_test(output_dir=OUT)
    print("\nDone.")
