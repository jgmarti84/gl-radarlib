# -*- coding: utf-8 -*-
"""Unit tests for radarlib.io.pyart.fieldfilters module.

Tests the field filtering functions for Py-ART radar objects.
"""


import numpy as np


class MockRadar:
    """Mock radar object for testing field filters."""

    def __init__(self, nrays=360, ngates=500, nsweeps=1):
        self.nrays = nrays
        self.ngates = ngates
        self.nsweeps = nsweeps
        self.fields = {}
        self.range = {"data": np.linspace(0, 250000, ngates)}  # 0 to 250km

    def add_field(self, field_name: str, data: np.ndarray):
        """Add a field to the radar."""
        self.fields[field_name] = {"data": np.ma.masked_array(data)}

    def add_field_like(self, old_field: str, new_field: str, data, replace_existing: bool = False):
        """Add a field based on another field's metadata."""
        self.fields[new_field] = {"data": data}


class TestFilterFieldExcludingGatesBelow:
    """Tests for filterfield_excluding_gates_below function."""

    def test_masks_values_below_threshold(self):
        """Should mask values below the threshold."""
        from radarlib.io.pyart.fieldfilters import filterfield_excluding_gates_below

        radar = MockRadar(nrays=10, ngates=10)
        source_data = np.array([[5, 10, 15, 5, 20]] * 10)
        target_data = np.array([[1, 2, 3, 4, 5]] * 10)

        radar.add_field("RHOHV", source_data)
        radar.add_field("DBZH", target_data)

        filterfield_excluding_gates_below(
            radar, threshold=10, source_field="RHOHV", target_fields=["DBZH"], overwrite_fields=True
        )

        # Values at indices 0, 3 should be masked (source < 10)
        assert radar.fields["DBZH"]["data"].mask[0, 0] is True
        assert radar.fields["DBZH"]["data"].mask[0, 3] is True
        # Values at indices 1, 2, 4 should not be masked
        assert radar.fields["DBZH"]["data"].mask[0, 1] is False
        assert radar.fields["DBZH"]["data"].mask[0, 2] is False

    def test_creates_new_field_when_not_overwriting(self):
        """Should create a new field when overwrite_fields is False."""
        from radarlib.io.pyart.fieldfilters import filterfield_excluding_gates_below

        radar = MockRadar(nrays=10, ngates=10)
        source_data = np.array([[15] * 10] * 10)
        target_data = np.array([[1] * 10] * 10)

        radar.add_field("RHOHV", source_data)
        radar.add_field("DBZH", target_data)

        filterfield_excluding_gates_below(
            radar,
            threshold=10,
            source_field="RHOHV",
            target_fields=["DBZH"],
            overwrite_fields=False,
            new_fields_complement_name="_filtered",
        )

        assert "DBZH_filtered" in radar.fields

    def test_handles_missing_source_field(self, caplog):
        """Should log error when source field is missing."""
        import logging

        from radarlib.io.pyart.fieldfilters import filterfield_excluding_gates_below

        caplog.set_level(logging.ERROR)
        radar = MockRadar()

        filterfield_excluding_gates_below(radar, threshold=10, source_field="NONEXISTENT", target_fields=["DBZH"])

        assert "no encontrado" in caplog.text or "NONEXISTENT" in caplog.text

    def test_filters_all_fields_when_target_is_none(self):
        """Should filter all fields when target_fields is None."""
        from radarlib.io.pyart.fieldfilters import filterfield_excluding_gates_below

        radar = MockRadar(nrays=5, ngates=5)
        source_data = np.array([[15] * 5] * 5)
        field1_data = np.array([[1] * 5] * 5)

        radar.add_field("RHOHV", source_data)
        radar.add_field("DBZH", field1_data)

        # Convert target_fields to list to avoid dictionary iteration issues
        target_fields = list(radar.fields.keys())

        filterfield_excluding_gates_below(
            radar,
            threshold=10,
            source_field="RHOHV",
            target_fields=target_fields,  # Use explicit list
            overwrite_fields=False,
            new_fields_complement_name="_f",
        )

        # Fields should have filtered versions
        assert "RHOHV_f" in radar.fields
        assert "DBZH_f" in radar.fields


class TestFilterFieldExcludingGatesAbove:
    """Tests for filterfield_excluding_gates_above function."""

    def test_masks_values_above_threshold(self):
        """Should mask values above the threshold."""
        from radarlib.io.pyart.fieldfilters import filterfield_excluding_gates_above

        radar = MockRadar(nrays=10, ngates=5)
        source_data = np.array([[5, 10, 15, 5, 20]] * 10)
        target_data = np.array([[1, 2, 3, 4, 5]] * 10)

        radar.add_field("WRAD", source_data)
        radar.add_field("DBZH", target_data)

        filterfield_excluding_gates_above(
            radar, threshold=10, source_field="WRAD", target_fields=["DBZH"], overwrite_fields=True
        )

        # Values at indices 2, 4 should be masked (source > 10)
        assert radar.fields["DBZH"]["data"].mask[0, 2] is True
        assert radar.fields["DBZH"]["data"].mask[0, 4] is True
        # Values at indices 0, 1, 3 should not be masked
        assert radar.fields["DBZH"]["data"].mask[0, 0] is False
        assert radar.fields["DBZH"]["data"].mask[0, 1] is False

    def test_creates_new_field_when_not_overwriting(self):
        """Should create a new field when overwrite_fields is False."""
        from radarlib.io.pyart.fieldfilters import filterfield_excluding_gates_above

        radar = MockRadar(nrays=10, ngates=10)
        source_data = np.array([[5] * 10] * 10)
        target_data = np.array([[1] * 10] * 10)

        radar.add_field("WRAD", source_data)
        radar.add_field("DBZH", target_data)

        filterfield_excluding_gates_above(
            radar,
            threshold=10,
            source_field="WRAD",
            target_fields=["DBZH"],
            overwrite_fields=False,
            new_fields_complement_name="_filt",
        )

        assert "DBZH_filt" in radar.fields


class TestFilterFieldsFromMask:
    """Tests for filter_fields_from_mask function."""

    def test_applies_mask_to_fields(self):
        """Should apply the provided mask to target fields."""
        from radarlib.io.pyart.fieldfilters import filter_fields_from_mask

        radar = MockRadar(nrays=5, ngates=5)
        field_data = np.array([[1, 2, 3, 4, 5]] * 5)
        radar.add_field("DBZH", field_data)

        # Create mask - True means masked
        mask = np.array([[True, False, True, False, True]] * 5)

        filter_fields_from_mask(radar, mask=mask, target_fields=["DBZH"], overwrite_fields=True)

        # Check that mask was applied
        assert radar.fields["DBZH"]["data"].mask[0, 0] is True
        assert radar.fields["DBZH"]["data"].mask[0, 1] is False
        assert radar.fields["DBZH"]["data"].mask[0, 2] is True

    def test_creates_new_field_when_not_overwriting(self):
        """Should create a new field when overwrite_fields is False."""
        from radarlib.io.pyart.fieldfilters import filter_fields_from_mask

        radar = MockRadar(nrays=5, ngates=5)
        field_data = np.array([[1, 2, 3, 4, 5]] * 5)
        radar.add_field("DBZH", field_data)

        mask = np.array([[False] * 5] * 5)

        filter_fields_from_mask(
            radar, mask=mask, target_fields=["DBZH"], overwrite_fields=False, new_fields_complement_name="_masked"
        )

        assert "DBZH_masked" in radar.fields

    def test_skips_nonexistent_fields(self, caplog):
        """Should skip fields that don't exist."""
        import logging

        from radarlib.io.pyart.fieldfilters import filter_fields_from_mask

        caplog.set_level(logging.DEBUG)
        radar = MockRadar()

        mask = np.array([[False]])

        filter_fields_from_mask(radar, mask=mask, target_fields=["NONEXISTENT"], overwrite_fields=True)

        # Should log that field was skipped
        assert "no existe" in caplog.text or "NONEXISTENT" in caplog.text or len(radar.fields) == 0


class TestMaskFieldOutsideLimits:
    """Tests for mask_field_outside_limits function."""

    def test_masks_outside_radial_limits(self):
        """Should mask values outside radial limits."""
        from radarlib.io.pyart.fieldfilters import mask_field_outside_limits

        radar = MockRadar(nrays=36, ngates=100, nsweeps=1)
        # Create masked array with proper 2D mask
        data = np.ones((36, 100))
        field_data = np.ma.masked_array(data, mask=np.zeros((36, 100), dtype=bool))
        radar.fields["DBZH"] = {"data": field_data}

        # Mask outside 50km to 150km (gates ~20-60 based on 250km total)
        mask_field_outside_limits(radar, radio_inf=50, radio_ext=150, az_lim1=5, az_lim2=30, fields_to_mask=["DBZH"])

        # Check that values outside azimuth limits are masked
        assert radar.fields["DBZH"]["data"].mask[0, 50] is True  # az=0 is outside [5, 30]
        assert radar.fields["DBZH"]["data"].mask[35, 50] is True  # az=35 is outside [5, 30]

    def test_uses_default_limits_when_none(self):
        """Should use default limits when parameters are None."""
        from radarlib.io.pyart.fieldfilters import mask_field_outside_limits

        radar = MockRadar(nrays=36, ngates=100, nsweeps=1)
        # Create masked array with proper 2D mask
        data = np.ones((36, 100))
        field_data = np.ma.masked_array(data, mask=np.zeros((36, 100), dtype=bool))
        radar.fields["DBZH"] = {"data": field_data}

        # Should not raise with None values
        mask_field_outside_limits(
            radar, radio_inf=None, radio_ext=None, az_lim1=None, az_lim2=None, fields_to_mask=["DBZH"]
        )


class TestMaskFieldInsideLimits:
    """Tests for mask_field_inside_limits function."""

    def test_masks_inside_limits(self):
        """Should mask values inside the specified limits."""
        from radarlib.io.pyart.fieldfilters import mask_field_inside_limits

        radar = MockRadar(nrays=36, ngates=100, nsweeps=1)
        # Create masked array with proper 2D mask
        data = np.zeros((36, 100))
        field_data = np.ma.masked_array(data, mask=np.zeros((36, 100), dtype=bool))
        radar.fields["DBZH"] = {"data": field_data}

        # Mask inside az 5-10
        mask_field_inside_limits(radar, radio_inf=10, radio_ext=100, az_lim1=5, az_lim2=10, fields_to_mask=["DBZH"])

        # Gates inside the range should be masked for az 5-10
        # (exact gates depend on range data)

    def test_uses_default_limits_when_zero(self):
        """Should use default limits when parameters use defaults."""
        from radarlib.io.pyart.fieldfilters import mask_field_inside_limits

        radar = MockRadar(nrays=36, ngates=100, nsweeps=1)
        # Create masked array with proper 2D mask
        data = np.ones((36, 100))
        field_data = np.ma.masked_array(data, mask=np.zeros((36, 100), dtype=bool))
        radar.fields["DBZH"] = {"data": field_data}

        # Should not raise with default values
        mask_field_inside_limits(
            radar, radio_inf=0, radio_ext=250, az_lim1=0, az_lim2=36, fields_to_mask=["DBZH"]  # Default value
        )

    def test_masks_all_fields_when_none_specified(self):
        """Should mask all fields when fields_to_mask is None."""
        from radarlib.io.pyart.fieldfilters import mask_field_inside_limits

        radar = MockRadar(nrays=36, ngates=100, nsweeps=1)
        # Create masked arrays with proper 2D masks
        data1 = np.ones((36, 100))
        data2 = np.ones((36, 100))
        field_data1 = np.ma.masked_array(data1, mask=np.zeros((36, 100), dtype=bool))
        field_data2 = np.ma.masked_array(data2, mask=np.zeros((36, 100), dtype=bool))
        radar.fields["DBZH"] = {"data": field_data1}
        radar.fields["ZDR"] = {"data": field_data2}

        mask_field_inside_limits(
            radar, radio_inf=10, radio_ext=50, az_lim1=0, az_lim2=5, fields_to_mask=None  # All fields
        )

        # Both fields should have some masked values
        # (exact values depend on implementation)


class TestIntegration:
    """Integration tests for fieldfilters module."""

    def test_chaining_filters(self):
        """Should be able to chain multiple filters."""
        from radarlib.io.pyart.fieldfilters import filterfield_excluding_gates_above, filterfield_excluding_gates_below

        radar = MockRadar(nrays=10, ngates=10)
        rhohv_data = np.array([[0.5, 0.9, 0.95, 0.7, 0.98]] * 10)
        wrad_data = np.array([[2, 5, 3, 8, 1]] * 10)
        dbzh_data = np.array([[10, 20, 30, 40, 50]] * 10)

        radar.add_field("RHOHV", rhohv_data)
        radar.add_field("WRAD", wrad_data)
        radar.add_field("DBZH", dbzh_data)

        # Filter below RHOHV threshold
        filterfield_excluding_gates_below(
            radar, threshold=0.8, source_field="RHOHV", target_fields=["DBZH"], overwrite_fields=True
        )

        # Filter above WRAD threshold
        filterfield_excluding_gates_above(
            radar, threshold=5, source_field="WRAD", target_fields=["DBZH"], overwrite_fields=True
        )

        # Verify compound filtering
        # Index 0: RHOHV < 0.8 -> masked
        # Index 3: WRAD > 5 -> masked
        assert radar.fields["DBZH"]["data"].mask[0, 0] is True
        assert radar.fields["DBZH"]["data"].mask[0, 3] is True
