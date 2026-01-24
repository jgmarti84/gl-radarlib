# -*- coding: utf-8 -*-
"""Unit tests for radarlib.io.pyart.radar_png_plotter module.

Tests the radar PNG plotting utilities.
"""
import os
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from radarlib.io.pyart.radar_png_plotter import (
    FieldPlotConfig,
    RadarPlotConfig,
    plot_and_save_ppi,
    plot_fields_with_substitution,
    plot_multiple_fields,
    plot_ppi_field,
    save_ppi_png,
    setup_plot_figure,
)


class MockRadar:
    """Mock PyART Radar object for testing."""

    def __init__(self, fields=None, nsweeps=4):
        if fields is None:
            fields = ["DBZH", "VRAD", "RHOHV", "KDP"]

        self.nsweeps = nsweeps
        self.fields = {}

        # Create mock field data
        for field in fields:
            self.fields[field] = {
                "data": np.ma.array(np.random.random((360, 500)), dtype=np.float32),
                "units": "dBZ" if "DBZ" in field else "m/s",
                "standard_name": field,
            }

        # Mock radar metadata
        self.latitude = {"data": np.array([-34.5])}
        self.longitude = {"data": np.array([-58.5])}
        self.altitude = {"data": np.array([100.0])}
        self.range = {"data": np.arange(0, 250000, 500)}
        self.azimuth = {"data": np.linspace(0, 360, 360)}
        self.elevation = {"data": np.array([0.5, 1.0, 1.5, 2.0])}


class TestRadarPlotConfig:
    """Tests for RadarPlotConfig class."""

    def test_init_with_defaults(self):
        """Should initialize with default parameters."""
        config = RadarPlotConfig()

        assert config.figsize == (12, 12)
        assert config.dpi == 150
        assert config.transparent is True
        assert config.colorbar is False
        assert config.title is False
        assert config.axis_labels is False
        assert config.tight_layout is True

    def test_init_with_custom_params(self):
        """Should initialize with custom parameters."""
        config = RadarPlotConfig(
            figsize=(10, 8),
            dpi=300,
            transparent=False,
            colorbar=True,
            title=True,
            axis_labels=True,
            tight_layout=False,
        )

        assert config.figsize == (10, 8)
        assert config.dpi == 300
        assert config.transparent is False
        assert config.colorbar is True
        assert config.title is True
        assert config.axis_labels is True
        assert config.tight_layout is False


class TestFieldPlotConfig:
    """Tests for FieldPlotConfig class."""

    def test_init_with_field_name(self):
        """Should initialize with field name and defaults."""
        config = FieldPlotConfig("DBZH")

        assert config.field_name == "DBZH"
        assert config.sweep == 0
        assert config.gatefilter is None

    def test_init_with_custom_vmin_vmax(self):
        """Should use custom vmin and vmax."""
        config = FieldPlotConfig("VRAD", vmin=-30, vmax=30)

        assert config.vmin == -30
        assert config.vmax == 30

    def test_init_with_custom_cmap(self):
        """Should use custom colormap."""
        config = FieldPlotConfig("RHOHV", cmap="viridis")

        assert config.cmap == "viridis"

    def test_init_with_custom_sweep(self):
        """Should use custom sweep."""
        config = FieldPlotConfig("KDP", sweep=2)

        assert config.sweep == 2


class TestSetupPlotFigure:
    """Tests for setup_plot_figure function."""

    @patch("radarlib.io.pyart.radar_png_plotter.plt")
    def test_setup_creates_figure(self, mock_plt):
        """Should create figure with correct configuration."""
        mock_fig = MagicMock()
        mock_ax = MagicMock()
        mock_ax.spines.values.return_value = []
        mock_fig.add_subplot.return_value = mock_ax
        mock_plt.figure.return_value = mock_fig

        config = RadarPlotConfig(figsize=(10, 10))
        fig, ax = setup_plot_figure(config)

        mock_plt.figure.assert_called_once_with(figsize=(10, 10))
        mock_plt.ioff.assert_called_once()

    @patch("radarlib.io.pyart.radar_png_plotter.plt")
    def test_setup_removes_axis_spines(self, mock_plt):
        """Should set axis off."""
        mock_fig = MagicMock()
        mock_ax = MagicMock()
        mock_ax.spines.values.return_value = [MagicMock(), MagicMock()]
        mock_fig.add_subplot.return_value = mock_ax
        mock_plt.figure.return_value = mock_fig

        config = RadarPlotConfig()
        setup_plot_figure(config)

        mock_plt.axis.assert_called_with("off")


class TestPlotPPIField:
    """Tests for plot_ppi_field function."""

    def test_plot_ppi_field_raises_on_missing_field(self):
        """Should raise ValueError if field not in radar."""
        radar = MockRadar(fields=["DBZH"])

        with pytest.raises(ValueError, match="Field 'INVALID' not found"):
            plot_ppi_field(radar, "INVALID")

    def test_plot_ppi_field_raises_on_invalid_sweep(self):
        """Should raise ValueError if sweep out of range."""
        radar = MockRadar(fields=["DBZH"], nsweeps=3)

        with pytest.raises(ValueError, match="Sweep 5 out of range"):
            plot_ppi_field(radar, "DBZH", sweep=5)

    @patch("radarlib.io.pyart.radar_png_plotter.pyart.graph.RadarDisplay")
    @patch("radarlib.io.pyart.radar_png_plotter.setup_plot_figure")
    def test_plot_ppi_field_creates_display(self, mock_setup, mock_display_class):
        """Should create radar display and plot."""
        mock_fig = MagicMock()
        mock_ax = MagicMock()
        mock_setup.return_value = (mock_fig, mock_ax)

        mock_display = MagicMock()
        mock_display_class.return_value = mock_display

        radar = MockRadar(fields=["DBZH"])

        fig, ax = plot_ppi_field(radar, "DBZH")

        mock_display_class.assert_called_once_with(radar)
        mock_display.plot_ppi.assert_called_once()


class TestSavePPIPng:
    """Tests for save_ppi_png function."""

    def test_save_ppi_png_creates_directory(self, tmp_path):
        """Should create output directory if it doesn't exist."""
        mock_fig = MagicMock()
        output_dir = tmp_path / "new_dir" / "output"

        save_ppi_png(mock_fig, str(output_dir), "test.png")

        assert output_dir.exists()

    def test_save_ppi_png_returns_full_path(self, tmp_path):
        """Should return full path to saved file."""
        mock_fig = MagicMock()

        result = save_ppi_png(mock_fig, str(tmp_path), "test.png")

        assert result == os.path.join(str(tmp_path), "test.png")

    def test_save_ppi_png_calls_savefig(self, tmp_path):
        """Should call savefig with correct parameters."""
        mock_fig = MagicMock()

        save_ppi_png(mock_fig, str(tmp_path), "test.png", dpi=300, transparent=False)

        mock_fig.savefig.assert_called_once()
        call_kwargs = mock_fig.savefig.call_args[1]
        assert call_kwargs["dpi"] == 300
        assert call_kwargs["transparent"] is False


class TestPlotAndSavePPI:
    """Tests for plot_and_save_ppi function."""

    @patch("radarlib.io.pyart.radar_png_plotter.plt")
    @patch("radarlib.io.pyart.radar_png_plotter.plot_ppi_field")
    @patch("radarlib.io.pyart.radar_png_plotter.save_ppi_png")
    def test_plot_and_save_ppi_returns_path(self, mock_save, mock_plot, mock_plt, tmp_path):
        """Should return path to saved file."""
        mock_fig = MagicMock()
        mock_ax = MagicMock()
        mock_plot.return_value = (mock_fig, mock_ax)
        mock_save.return_value = str(tmp_path / "output.png")

        radar = MockRadar(fields=["DBZH"])

        result = plot_and_save_ppi(radar, "DBZH", str(tmp_path), "output.png")

        assert result == str(tmp_path / "output.png")
        mock_plt.close.assert_called_with(mock_fig)

    @patch("radarlib.io.pyart.radar_png_plotter.plt")
    @patch("radarlib.io.pyart.radar_png_plotter.plot_ppi_field")
    @patch("radarlib.io.pyart.radar_png_plotter.save_ppi_png")
    def test_plot_and_save_ppi_closes_figure(self, mock_save, mock_plot, mock_plt, tmp_path):
        """Should close figure after saving."""
        mock_fig = MagicMock()
        mock_ax = MagicMock()
        mock_plot.return_value = (mock_fig, mock_ax)

        radar = MockRadar(fields=["DBZH"])

        plot_and_save_ppi(radar, "DBZH", str(tmp_path), "output.png")

        mock_plt.close.assert_called_with(mock_fig)


class TestPlotMultipleFields:
    """Tests for plot_multiple_fields function."""

    @patch("radarlib.io.pyart.radar_png_plotter.plot_and_save_ppi")
    def test_plot_multiple_fields_returns_results_dict(self, mock_plot_save, tmp_path):
        """Should return dictionary of field to path mappings."""
        mock_plot_save.side_effect = [
            str(tmp_path / "DBZH_sweep00.png"),
            str(tmp_path / "VRAD_sweep00.png"),
        ]

        radar = MockRadar(fields=["DBZH", "VRAD"])

        results = plot_multiple_fields(radar, ["DBZH", "VRAD"], str(tmp_path))

        assert "DBZH" in results
        assert "VRAD" in results
        assert len(results) == 2

    @patch("radarlib.io.pyart.radar_png_plotter.plot_and_save_ppi")
    def test_plot_multiple_fields_skips_missing_fields(self, mock_plot_save, tmp_path):
        """Should skip fields not in radar."""
        mock_plot_save.return_value = str(tmp_path / "DBZH_sweep00.png")

        radar = MockRadar(fields=["DBZH"])

        results = plot_multiple_fields(radar, ["DBZH", "NONEXISTENT"], str(tmp_path))

        assert "DBZH" in results
        assert "NONEXISTENT" not in results

    @patch("radarlib.io.pyart.radar_png_plotter.plot_and_save_ppi")
    def test_plot_multiple_fields_handles_errors(self, mock_plot_save, tmp_path):
        """Should continue on error for a single field."""
        mock_plot_save.side_effect = [
            Exception("Plot error"),
            str(tmp_path / "VRAD_sweep00.png"),
        ]

        radar = MockRadar(fields=["DBZH", "VRAD"])

        results = plot_multiple_fields(radar, ["DBZH", "VRAD"], str(tmp_path))

        # DBZH should fail, VRAD should succeed
        assert "DBZH" not in results
        assert "VRAD" in results


class TestPlotFieldsWithSubstitution:
    """Tests for plot_fields_with_substitution function."""

    @patch("radarlib.io.pyart.radar_png_plotter.plot_and_save_ppi")
    def test_uses_original_field_when_present(self, mock_plot_save, tmp_path):
        """Should use original field when it exists."""
        mock_plot_save.return_value = str(tmp_path / "DBZH.png")

        radar = MockRadar(fields=["DBZH", "TH"])
        substitutions = {"DBZH": "TH"}

        results = plot_fields_with_substitution(radar, ["DBZH"], str(tmp_path), field_substitutions=substitutions)
        assert results

        # Should plot DBZH since it exists
        assert mock_plot_save.called
        call_args = mock_plot_save.call_args
        assert call_args[0][1] == "DBZH"  # field argument

    @patch("radarlib.io.pyart.radar_png_plotter.plot_and_save_ppi")
    def test_uses_substitute_when_original_missing(self, mock_plot_save, tmp_path):
        """Should use substitute field when original doesn't exist."""
        mock_plot_save.return_value = str(tmp_path / "TH.png")

        radar = MockRadar(fields=["TH"])  # No DBZH
        substitutions = {"DBZH": "TH"}

        results = plot_fields_with_substitution(radar, ["DBZH"], str(tmp_path), field_substitutions=substitutions)
        assert results

        # Should plot TH as substitute
        assert mock_plot_save.called
        call_args = mock_plot_save.call_args
        assert call_args[0][1] == "TH"  # field argument


class TestIntegration:
    """Integration tests for complete workflows."""

    def test_config_objects_are_independent(self):
        """Config objects should not share state."""
        config1 = RadarPlotConfig(dpi=100)
        config2 = RadarPlotConfig(dpi=300)

        assert config1.dpi == 100
        assert config2.dpi == 300

    def test_field_config_stores_all_params(self):
        """FieldPlotConfig should store all parameters."""
        gatefilter = MagicMock()
        config = FieldPlotConfig(
            "DBZH",
            vmin=-10,
            vmax=80,
            cmap="jet",
            sweep=2,
            gatefilter=gatefilter,
        )

        assert config.field_name == "DBZH"
        assert config.vmin == -10
        assert config.vmax == 80
        assert config.cmap == "jet"
        assert config.sweep == 2
        assert config.gatefilter is gatefilter
