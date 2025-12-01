import logging

import numpy as np
import pyart
from pyart.config import get_field_name

from radarlib.io.pyart.fieldfilters import filter_fields_from_mask

logger = logging.getLogger(__name__)


def filter_fields_grc1(
    radar,
    refl_field=None,
    refl_filter=True,
    refl_threshold=-3,
    rhv_field=None,
    rhv_filter1=True,
    rhv_threshold1=0.55,
    zdr_field=None,
    zdr_filter=True,
    zdr_threshold=8.5,
    wrad_field=None,
    wrad_filter=True,
    wrad_threshold=4.6,
    refl_filter2=True,
    refl_threshold2=25,
    cm_field=None,
    cm_filter=True,
    rhohv_threshold2=0.85,
    despeckle_filter=True,
    size=80,
    mean_filter=True,
    mean_threshold=0.85,
    # logger_name=__name__,
    verbose=False,
    target_fields=None,
    overwrite_fields=False,
    new_fields_complement_name="f",
):

    # logger = logging.getLogger(logger_name)

    if refl_field is None:
        refl_field = get_field_name("reflectivity")
    if rhv_field is None:
        rhv_field = get_field_name("cross_correlation_ratio")
    if zdr_field is None:
        zdr_field = get_field_name("differential_reflectivity")
    if wrad_field is None:
        wrad_field = get_field_name("spectrum_width")
    if cm_field is None:
        cm_field = get_field_name("clutter_map")
    if target_fields is None:
        if refl_field in radar.fields.keys():
            target_fields = [
                refl_field,
            ]
        else:
            target_fields = []

    # 1) Máscara General
    gatefilter = pyart.correct.GateFilter(radar)
    if refl_filter:
        if refl_field in radar.fields.keys():
            gatefilter.exclude_below(refl_field, refl_threshold)
            logger.debug("Enmascarando " + refl_field + " inferiores a " + str(refl_threshold)) if verbose else None
        else:
            logger.debug(refl_field + " inexistente, no se utiliza en filtro.")

    if rhv_filter1:
        if rhv_field in radar.fields.keys():
            gatefilter.exclude_below(rhv_field, rhv_threshold1)
            logger.debug("Enmascarando " + rhv_field + " inferiores a " + str(rhv_threshold1)) if verbose else None
        else:
            logger.debug(rhv_field + " inexistente, no se utiliza en filtro.")

    if zdr_filter:
        if zdr_field in radar.fields.keys():
            gatefilter.exclude_above(zdr_field, zdr_threshold)
            logger.debug("Enmascarando " + zdr_field + " superiores a " + str(zdr_threshold)) if verbose else None
        else:
            logger.debug(zdr_field + " inexistente, no se utiliza en filtro.")

    if wrad_filter:
        if wrad_field in radar.fields.keys():
            gatefilter.exclude_above(wrad_field, wrad_threshold)
            logger.debug("Enmascarando " + wrad_field + " superiores a " + str(wrad_threshold)) if verbose else None
        else:
            logger.debug(wrad_field + " inexistente, no se utiliza en filtro.")

    # 2) Reincorporamos los valores superiores a threshold
    if refl_filter2:
        gatefilter.include_above(refl_field, refl_threshold2) if refl_field in radar.fields.keys() else None

    # 3) Determinamos máscara con valores No Meteorológicos de Mapa de
    #   Clutter y se la restamos a máscara general
    if cm_filter and cm_field in radar.fields.keys():
        gatefilter2 = pyart.correct.GateFilter(radar, exclude_based=False)
        gatefilter2.include_equal(cm_field, 1.0)
        gatefilter2.exclude_above(rhv_field, rhohv_threshold2) if rhv_field in radar.fields.keys() else None
        gatefilter.exclude_gates(gatefilter2.gate_included)  # restamos mask
        logger.debug("Enmascarando aplicando ClutterMap") if verbose else None

    # 4) Filtrado por tamaño y media de RHOHV en conjunto de celdas
    # label_dict = pyart.correct.despeckle.find_objects(
    # radar, field='RHOHV', threshold=-100, sweeps=None,
    # smooth=None, gatefilter=gatefilter, delta=5.0)
    if despeckle_filter and rhv_field in radar.fields.keys():
        gatefilter = despeckle_field_with_mean_threshold(
            radar,
            field=rhv_field,
            label_dict=None,
            threshold=-100,
            size=size,
            gatefilter=gatefilter,
            delta=5.0,
            mean_filter=mean_filter,
            mean_threshold=mean_threshold,
        )  # type: ignore
        logger.debug("Enmascarando por tamaño y media del RHOHV" + " del conjunto de celdas.") if verbose else None

    # 5) Guardamos nuevo campo filtrado
    filter_fields_from_mask(
        radar,
        mask=gatefilter.gate_excluded,
        target_fields=target_fields,
        overwrite_fields=overwrite_fields,
        new_fields_complement_name=new_fields_complement_name,
        # logger_name=logger_name,
    )
    logger.debug("Generando nuevos campos filtrados.") if (verbose and not overwrite_fields) else None
    logger.debug("Sobreescribiendo campos con datos filtrados.") if (verbose and overwrite_fields) else None

    return gatefilter


def despeckle_field_with_mean_threshold(
    radar,
    field,
    label_dict=None,
    threshold: float | int = -100,
    size: float | int = 10,
    gatefilter=None,
    delta: float = 5.0,
    mean_filter: bool = True,
    mean_threshold: float | int = -100,
):
    """
    Despeckle a radar volume by identifying small objects in each scan and
    masking them out. User can define which field to investigate, as well as
    various thresholds to use on that field and any objects found within.
    Requires scipy to be installed, and returns a GateFilter object.

    Parameters
    ----------
    radar : pyart.core.Radar object
        Radar object to query.
    field : str
        Name of field to investigate for speckles.

    Other Parameters
    ----------------
    label_dict : dict or None, optional
        Dictionary that is produced by find_objects.
        If None, find_objects will be called to produce it.
    threshold : int or float, or 2-element tuple of ints or floats
        Threshold values above (if single value) or between (if tuple)
        for objects to be identified. Default value assumes reflectivity.
    size : int, optional
        Number of contiguous gates in an object, below which it is a speckle.
    gatefilter : None or pyart.filters.GateFilter object
        Py-ART GateFilter object to which to add the despeckling mask. The
        GateFilter object will be permanently modified with the new filtering.
        If None, creates a new GateFilter.
    delta : int or float, optional
        Size of allowable gap near PPI edges, in deg, to consider it full 360.
        If gap is small, then PPI edges will be checked for matching objects.

    Returns
    -------
    gatefilter : pyart.filters.GateFilter object
        Py-ART GateFilter object that includes the despeckling mask

    """

    BAD = 1e20  # Absurdly unphysical value, for easy thresholding
    from pyart.correct.despeckle import find_objects
    from pyart.filters.gatefilter import GateFilter

    if field not in radar.fields.keys():
        raise KeyError("Failed -", field, "field not found in Radar object.")
    if label_dict is None:
        # Label everything in the radar object's field
        label_dict = find_objects(radar, field, threshold, gatefilter=gatefilter, delta=delta)
    if gatefilter is None:
        gatefilter = GateFilter(radar)
    labels = label_dict["data"]

    # Get a copy of the field in the volume
    data = 1.0 * radar.fields[field]["data"]
    mask_filter = gatefilter.gate_excluded
    data = np.ma.masked_array(data, mask_filter)
    data = data.filled(fill_value=BAD)
    labf = labels.filled(fill_value=0)

    # First reduce array size to speed up processing
    cond1 = np.logical_and(data != BAD, labf > 0)
    labr = labf[cond1]
    data_r = data[cond1]

    # Now loop thru all objects in volume, mask ones that are too small
    # These are the speckles
    iterarray = np.unique(labr)
    for _, lab in enumerate(iterarray):
        cond = labr == lab
        if np.size(labr[cond]) < size:
            data_r[cond] = BAD
        else:
            if mean_filter and np.mean(data_r[cond]) < mean_threshold:
                data_r[cond] = BAD

    data[cond1] = data_r
    data = np.ma.masked_where(data == BAD, data)
    gatefilter.exclude_gates(data.mask)
    return gatefilter
