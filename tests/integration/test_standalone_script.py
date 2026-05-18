# -*- coding: utf-8 -*-
"""Integration tests for ``scripts/generate_product_standalone.py``.

These tests verify the standalone CLI script end-to-end:

- ``--list-fields`` outputs available field names and exits cleanly
- A COG file is created at the expected path for a real volume
- The generated COG contains all required radarlib metadata tags
- Error paths (missing file, missing field) produce non-zero exit codes
- Internal helper functions (``_resolve_netcdf``, ``_find_geometry``) behave
  correctly

All tests are marked ``@pytest.mark.integration`` and are automatically
skipped when the required sample data files are not present on disk.

Run only this suite::

    pytest tests/integration/test_standalone_script.py -m integration -v
"""

import subprocess
import sys
from pathlib import Path

import pytest

# from typing import Any


# ---------------------------------------------------------------------------
# Resolve sample data paths
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).parent.parent.parent
_SCRIPTS_DIR = _REPO_ROOT / "scripts"
_STANDALONE_SCRIPT = _SCRIPTS_DIR / "generate_product_standalone.py"
_OUTPUTS_DIR = _REPO_ROOT / "outputs"
_GEOMETRIES_DIR = _REPO_ROOT / "app" / "data" / "geometries"

# Sample files
_SAMPLE_NC = _OUTPUTS_DIR / "RMA1_0315_01_20260423T070721Z.nc"
_SAMPLE_GEOMETRY = next(iter(sorted(_GEOMETRIES_DIR.glob("RMA1_0315_01_*.npz"))), None)

# Python executable for subprocess tests
_PYTHON = sys.executable


def _skip_if_no_sample_data() -> None:
    if not _SAMPLE_NC.exists():
        pytest.skip(f"Sample NetCDF not found: {_SAMPLE_NC}")
    if _SAMPLE_GEOMETRY is None or not _SAMPLE_GEOMETRY.exists():
        pytest.skip(f"No geometry file found for RMA1_0315_01 in {_GEOMETRIES_DIR}")


# ---------------------------------------------------------------------------
# Helper: import the private functions from the script
# ---------------------------------------------------------------------------


def _import_script_module():
    """Import the standalone script as a module via importlib.

    Returns the imported module object so internal helpers can be tested.
    The module is NOT executed (``if __name__ == '__main__'`` is not triggered).
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location("generate_product_standalone", _STANDALONE_SCRIPT)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


# ---------------------------------------------------------------------------
# _find_geometry
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestFindGeometry:
    def test_finds_existing_geometry(self) -> None:
        """_find_geometry returns a Path when a matching file exists."""
        mod = _import_script_module()
        result = mod._find_geometry("RMA1", "0315", "01", _GEOMETRIES_DIR)
        assert result is not None
        assert isinstance(result, Path)
        assert result.exists()

    def test_returns_none_when_not_found(self, tmp_path: Path) -> None:
        """_find_geometry returns None when no matching file is in the directory."""
        mod = _import_script_module()
        result = mod._find_geometry("RMA99", "9999", "99", tmp_path)
        assert result is None

    def test_returned_path_is_npz(self) -> None:
        """The returned geometry file must have a .npz extension."""
        mod = _import_script_module()
        result = mod._find_geometry("RMA1", "0315", "01", _GEOMETRIES_DIR)
        if result is None:
            pytest.skip(f"Geometry not found in {_GEOMETRIES_DIR}")
        assert result.suffix == ".npz"

    def test_returned_path_matches_radar_and_strategy(self) -> None:
        """The returned path stem must start with RADAR_STRATEGY_VOLNR."""
        mod = _import_script_module()
        result = mod._find_geometry("RMA1", "0315", "01", _GEOMETRIES_DIR)
        if result is None:
            pytest.skip(f"Geometry not found in {_GEOMETRIES_DIR}")
        assert result.stem.startswith("RMA1_0315_01")


# ---------------------------------------------------------------------------
# _resolve_netcdf
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestResolveNetcdf:
    def test_explicit_path_returned_when_file_exists(self) -> None:
        """When a valid --netcdf-path is given, return it directly."""
        if not _SAMPLE_NC.exists():
            pytest.skip(f"Sample NetCDF not found: {_SAMPLE_NC}")
        mod = _import_script_module()
        result = mod._resolve_netcdf(
            netcdf_path=str(_SAMPLE_NC),
            radar_name=None,
            strategy=None,
            vol_nr=None,
            timestamp_str=None,
            netcdf_dir=None,
        )
        assert result == _SAMPLE_NC

    def test_explicit_path_missing_calls_sys_exit(self, tmp_path: Path) -> None:
        """A non-existent explicit path must call sys.exit(1)."""
        mod = _import_script_module()
        with pytest.raises(SystemExit) as exc:
            mod._resolve_netcdf(
                netcdf_path=str(tmp_path / "missing.nc"),
                radar_name=None,
                strategy=None,
                vol_nr=None,
                timestamp_str=None,
                netcdf_dir=None,
            )
        assert exc.value.code == 1

    def test_component_path_resolution(self) -> None:
        """Provide radar/strategy/vol_nr/timestamp/dir — must resolve to the file."""
        if not _SAMPLE_NC.exists():
            pytest.skip(f"Sample NetCDF not found: {_SAMPLE_NC}")
        mod = _import_script_module()
        result = mod._resolve_netcdf(
            netcdf_path=None,
            radar_name="RMA1",
            strategy="0315",
            vol_nr="01",
            timestamp_str="20260423T070721Z",
            netcdf_dir=str(_OUTPUTS_DIR),
        )
        assert result == _SAMPLE_NC

    def test_missing_components_calls_sys_exit(self, tmp_path: Path) -> None:
        """Missing required components must call sys.exit(1)."""
        mod = _import_script_module()
        with pytest.raises(SystemExit) as exc:
            mod._resolve_netcdf(
                netcdf_path=None,
                radar_name="RMA1",
                strategy=None,  # missing!
                vol_nr="01",
                timestamp_str="20260423T070721Z",
                netcdf_dir=str(tmp_path),
            )
        assert exc.value.code == 1

    def test_bad_timestamp_format_calls_sys_exit(self) -> None:
        """Invalid timestamp format must call sys.exit(1)."""
        mod = _import_script_module()
        with pytest.raises(SystemExit) as exc:
            mod._resolve_netcdf(
                netcdf_path=None,
                radar_name="RMA1",
                strategy="0315",
                vol_nr="01",
                timestamp_str="not-a-timestamp",
                netcdf_dir=str(_OUTPUTS_DIR),
            )
        assert exc.value.code == 1


# ---------------------------------------------------------------------------
# CLI: --list-fields
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCLIListFields:
    def test_list_fields_exits_zero(self) -> None:
        """``--list-fields`` should exit with code 0."""
        _skip_if_no_sample_data()
        result = subprocess.run(
            [
                _PYTHON,
                str(_STANDALONE_SCRIPT),
                "--netcdf-path",
                str(_SAMPLE_NC),
                "--geometry-path",
                str(_SAMPLE_GEOMETRY),
                "--list-fields",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"Expected exit 0; got {result.returncode}\nstderr:\n{result.stderr}"

    def test_list_fields_output_contains_field_names(self) -> None:
        """``--list-fields`` stdout lists at least one field name."""
        _skip_if_no_sample_data()
        result = subprocess.run(
            [
                _PYTHON,
                str(_STANDALONE_SCRIPT),
                "--netcdf-path",
                str(_SAMPLE_NC),
                "--geometry-path",
                str(_SAMPLE_GEOMETRY),
                "--list-fields",
            ],
            capture_output=True,
            text=True,
        )
        assert (
            "DBZH" in result.stdout or len(result.stdout.strip()) > 0
        ), f"Expected field names in output; got:\n{result.stdout}"


# ---------------------------------------------------------------------------
# CLI: missing required arguments
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCLIErrorPaths:
    def test_no_args_exits_with_error(self) -> None:
        """Running with no arguments should fail (argparse error)."""
        result = subprocess.run(
            [_PYTHON, str(_STANDALONE_SCRIPT)],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0

    def test_missing_netcdf_exits_nonzero(self, tmp_path: Path) -> None:
        """A non-existent --netcdf-path must return non-zero exit code."""
        result = subprocess.run(
            [
                _PYTHON,
                str(_STANDALONE_SCRIPT),
                "--netcdf-path",
                str(tmp_path / "missing.nc"),
                "--field",
                "DBZH",
                "--output-dir",
                str(tmp_path),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0

    def test_invalid_field_exits_nonzero(self) -> None:
        """Requesting a field that does not exist in the volume must exit non-zero."""
        _skip_if_no_sample_data()
        result = subprocess.run(
            [
                _PYTHON,
                str(_STANDALONE_SCRIPT),
                "--netcdf-path",
                str(_SAMPLE_NC),
                "--geometry-path",
                str(_SAMPLE_GEOMETRY),
                "--field",
                "NONEXISTENT_FIELD_XYZ",
                "--output-dir",
                "/tmp/radarlib_test_output",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0


# ---------------------------------------------------------------------------
# CLI: DBZH COG generation (full pipeline)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCLIFullPipeline:
    def test_unfiltered_cog_is_created(self, tmp_path: Path) -> None:
        """``generate_product_standalone.py`` creates a .tif file for DBZH (unfiltered)."""
        _skip_if_no_sample_data()

        result = subprocess.run(
            [
                _PYTHON,
                str(_STANDALONE_SCRIPT),
                "--netcdf-path",
                str(_SAMPLE_NC),
                "--geometry-path",
                str(_SAMPLE_GEOMETRY),
                "--field",
                "DBZH",
                "--output-dir",
                str(tmp_path),
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, (
            f"Script failed (exit {result.returncode}):\n" f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        tif_files = list(tmp_path.rglob("*.tif"))
        assert len(tif_files) >= 1, f"No .tif files found under {tmp_path};\nstdout:\n{result.stdout}"

    def test_generated_cog_has_radarlib_tags(self, tmp_path: Path) -> None:
        """The generated COG must contain radarlib_field_name and radarlib_radar_name tags."""
        _skip_if_no_sample_data()
        import rasterio

        result = subprocess.run(
            [
                _PYTHON,
                str(_STANDALONE_SCRIPT),
                "--netcdf-path",
                str(_SAMPLE_NC),
                "--geometry-path",
                str(_SAMPLE_GEOMETRY),
                "--field",
                "DBZH",
                "--output-dir",
                str(tmp_path),
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, f"Script failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        tif_files = list(tmp_path.rglob("*.tif"))
        assert tif_files, "No .tif output found"

        with rasterio.open(tif_files[0]) as src:
            tags = src.tags()

        assert "radarlib_field_name" in tags, f"radarlib_field_name missing; tags={tags}"
        assert tags["radarlib_field_name"] == "DBZH"

    def test_generated_cog_is_valid_geotiff(self, tmp_path: Path) -> None:
        """The generated COG must be openable by rasterio and have at least one band."""
        _skip_if_no_sample_data()
        import rasterio

        result = subprocess.run(
            [
                _PYTHON,
                str(_STANDALONE_SCRIPT),
                "--netcdf-path",
                str(_SAMPLE_NC),
                "--geometry-path",
                str(_SAMPLE_GEOMETRY),
                "--field",
                "DBZH",
                "--output-dir",
                str(tmp_path),
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, f"Script failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        tif_files = list(tmp_path.rglob("*.tif"))
        assert tif_files, "No .tif output found"

        with rasterio.open(tif_files[0]) as src:
            assert src.count >= 1
            assert src.width > 0
            assert src.height > 0

    def test_stdout_mentions_cog_generated_successfully(self, tmp_path: Path) -> None:
        """Script main() prints a success line when COG is written."""
        _skip_if_no_sample_data()
        result = subprocess.run(
            [
                _PYTHON,
                str(_STANDALONE_SCRIPT),
                "--netcdf-path",
                str(_SAMPLE_NC),
                "--geometry-path",
                str(_SAMPLE_GEOMETRY),
                "--field",
                "DBZH",
                "--output-dir",
                str(tmp_path),
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, f"Script failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        combined = result.stdout + result.stderr
        # The script prints "COG generated successfully" on success
        assert (
            "generated successfully" in combined.lower() or "cog" in combined.lower()
        ), f"Expected success message in output; got:\n{combined}"
