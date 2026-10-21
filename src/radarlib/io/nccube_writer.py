# -*- coding: utf-8 -*-
"""Write a Cartesian radar volume ("NC cube") to NetCDF4.

The cube is the 3D counterpart of the 2D COG products: the same interpolated
grid the product daemon already builds in memory, persisted instead of freed,
so downstream consumers (storm tracking, cross-sections, hydrometeor work) can
read a whole volume without re-running the interpolation.

Deliberately standalone — numpy and netCDF4 only, no radarlib import — so it
can be called from the product daemon without creating a dependency cycle, and
exercised in tests without constructing a daemon.

Row order
---------
`apply_geometry` returns grid[iz, iy, ix] with y increasing northward, so row 0
is the SOUTHERNMOST row. Consumers following the GDAL convention expect row 0
to be NORTH. This module flips the data on write and leaves y_km in its
original south-to-north order, because a reader taking y_km[-1] as the GDAL
origin pairs that with a negative dy step.

The flip happens HERE and nowhere else. Flipping again in the caller silently
mirrors the cube north-south, which no shape or range check will catch.

Vertical reference
------------------
`referencia_z` defaults to "radar" — heights above the antenna, not above sea
level. The chain that makes this so:

  1. radar_grid.utils.get_gate_coordinates returns radar.gate_z, which Py-ART
     defines as the gate height relative to the radar (gate_altitude is the
     absolute one).
  2. radar_grid.compute.compute_grid_geometry computes
     gate_z_relative = gate_z - radar_altitude, and every geometry built by the
     product daemon passes radar_altitude=0, making that a no-op.

So the cube's z axis stays antenna-relative, and a consumer converts a height
above sea level into that frame by subtracting the antenna altitude.

`radar_alt_m` must therefore be the REAL antenna altitude, taken from
radar.altitude. `GridGeometry.radar_altitude` is 0 for daemon-built geometries
and is NOT a substitute: writing 0, or omitting the attribute so a consumer
falls back to its own default, shifts every echo-height comparison by the
antenna altitude with no visible symptom.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from netCDF4 import Dataset

logger = logging.getLogger(__name__)

_FILL = np.float32(-9999.0)  # finite sentinel; consumers map it back to NaN

# Per-field CF metadata. Fields absent from this map are still written, with
# empty attributes and a warning — an unknown field is worth recording, but not
# worth failing a volume over.
FIELD_METADATA: dict[str, tuple[str, str]] = {
    "DBZH": ("Horizontal reflectivity factor", "dBZ"),
    "RHOHV": ("Cross-correlation ratio", "1"),
    "ZDR": ("Differential reflectivity", "dB"),
    "KDP": ("Specific differential phase", "deg/km"),
    "PHIDP": ("Differential phase", "deg"),
    "VRAD": ("Radial velocity", "m s-1"),
    "WRAD": ("Spectrum width", "m s-1"),
}


def write_nccube(
    path: Path | str,
    fields: dict[str, np.ndarray],
    x_km: np.ndarray,
    y_km: np.ndarray,
    z_m: np.ndarray,
    radar_lat: float,
    radar_lon: float,
    radar_alt_m: float,
    timestamp: datetime,
    referencia_z: str = "radar",
    extra_attrs: dict | None = None,
) -> Path:
    """Write one Cartesian radar volume as NetCDF4.

    Parameters
    ----------
    path          output file path; parent directories are created
    fields        {name: (nz, ny, nx) float array}, NaN = no data,
                  row 0 = SOUTH on input. Must be non-empty and all the same
                  shape.
    x_km          (nx,) km from the radar, increasing eastward
    y_km          (ny,) km from the radar, increasing northward, y[0] southernmost
    z_m           (nz,) height in metres in the frame named by referencia_z
    radar_lat     antenna latitude
    radar_lon     antenna longitude
    radar_alt_m   antenna altitude above sea level. Must be the REAL antenna
                  altitude (radar.altitude), not GridGeometry.radar_altitude.
    timestamp     volume observation time; naive values are read as UTC
    referencia_z  "radar" (above antenna) or "msnm" (above sea level)
    extra_attrs   optional extra global attributes (provenance, grid params)

    Returns the resolved output path.
    """
    if referencia_z not in ("radar", "msnm"):
        raise ValueError(f"referencia_z must be 'radar' or 'msnm', got {referencia_z!r}")
    if not fields:
        raise ValueError("fields is empty; nothing to write")

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    prepared: dict[str, np.ndarray] = {}
    shape = None
    for name, data in fields.items():
        arr = np.ma.filled(np.asarray(data, dtype=np.float32), np.nan)
        if arr.ndim != 3:
            raise ValueError(f"{name}: expected (nz, ny, nx), got {arr.shape}")
        if shape is None:
            shape = arr.shape
        elif arr.shape != shape:
            raise ValueError(f"shape mismatch: {name} {arr.shape} vs {shape} for the other field(s)")
        # row 0 south (apply_geometry) -> row 0 north (GDAL convention)
        prepared[name] = arr[:, ::-1, :]

    nz, ny, nx = shape
    x_km = np.asarray(x_km, dtype=np.float32)
    y_km = np.asarray(y_km, dtype=np.float32)  # left south-to-north on purpose
    z_m = np.asarray(z_m, dtype=np.float32)

    if (len(z_m), len(y_km), len(x_km)) != (nz, ny, nx):
        raise ValueError(
            f"coordinate lengths {(len(z_m), len(y_km), len(x_km))} " f"do not match data shape {(nz, ny, nx)}"
        )

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    with Dataset(str(path), "w", format="NETCDF4") as nc:
        nc.createDimension("z", nz)
        nc.createDimension("y", ny)
        nc.createDimension("x", nx)

        vz = nc.createVariable("z_m", "f4", ("z",))
        vz[:] = z_m
        vz.units = "m"
        vz.long_name = f"Height above {'antenna' if referencia_z == 'radar' else 'sea level'}"

        vy = nc.createVariable("y_km", "f4", ("y",))
        vy[:] = y_km
        vy.units = "km"
        vy.long_name = "Distance from radar northward (y[0]=south, y[-1]=north)"

        vx = nc.createVariable("x_km", "f4", ("x",))
        vx[:] = x_km
        vx.units = "km"
        vx.long_name = "Distance from radar eastward"

        for name, data in prepared.items():
            long_name, units = FIELD_METADATA.get(name, ("", ""))
            if not long_name:
                logger.warning("NC cube: no CF metadata for field %s; writing it without " "long_name/units", name)
            v = nc.createVariable(
                name,
                "f4",
                ("z", "y", "x"),
                fill_value=_FILL,
                zlib=True,
                complevel=1,
            )
            v[:] = np.where(np.isfinite(data), data, _FILL)
            v.long_name = long_name
            v.units = units

        nc.referencia_z = referencia_z
        nc.timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        nc.radar_lat = float(radar_lat)
        nc.radar_lon = float(radar_lon)
        nc.radar_alt_m = float(radar_alt_m)
        nc.row_order = "north_first"
        nc.fields = ",".join(prepared)
        nc.Conventions = "CF-1.8"
        nc.history = "Created by radarlib nccube_writer"

        for key, value in (extra_attrs or {}).items():
            setattr(nc, key, value)

    return path
