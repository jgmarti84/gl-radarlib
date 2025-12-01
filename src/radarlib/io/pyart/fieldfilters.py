# -*- coding: utf-8 -*-
"""
Field filtering functions for Py-ART radar objects.
"""
import logging

import numpy as np

logger = logging.getLogger(__name__)


def filterfield_excluding_gates_below(
    radar,
    threshold,
    source_field,
    target_fields=None,
    overwrite_fields=False,
    new_fields_complement_name="f",
    # logger_name="__name__",
    add_applied_filters_field=False,
):

    # logger = logging.getLogger(logger_name)

    if target_fields is None:
        target_fields = radar.fields.keys()

    if source_field in radar.fields.keys():

        for _i, field in enumerate(target_fields):

            # Se sobreescriben los campos filtrados
            if overwrite_fields:
                filtered_field = np.ma.masked_where(
                    radar.fields[source_field]["data"] < threshold, radar.fields[field]["data"]
                )

                # Agregamos el campo a los datos del radar.
                radar.add_field_like(field, field, filtered_field, replace_existing=True)

                if add_applied_filters_field:
                    if "applied_filters" in radar.fields[field].keys():
                        radar.fields[field]["applied_filters"] = (
                            radar.fields[field]["applied_filters"] + "[filt." + source_field + "]"
                        )
                    else:
                        radar.fields[field]["applied_filters"] = "[filt." + source_field + "]"

            # Generacion de Campos Filtrados
            else:
                filtered_field = np.ma.masked_where(
                    radar.fields[source_field]["data"] < threshold, radar.fields[field]["data"]
                )

                # Agregamos el campo a los datos del radar.
                radar.add_field_like(field, field + new_fields_complement_name, filtered_field, replace_existing=True)

                if add_applied_filters_field:
                    if "applied_filters" in radar.fields[field + new_fields_complement_name].keys():
                        radar.fields[field + new_fields_complement_name]["applied_filters"] = (
                            radar.fields[field]["applied_filters"] + "[filt." + source_field + "]"
                        )
                    else:
                        radar.fields[field + new_fields_complement_name]["applied_filters"] = (
                            "[filt." + source_field + "]"
                        )

    else:
        logger.error("Error en filtrado de campos. Field " + source_field + " no encontrado.")


def filterfield_excluding_gates_above(
    radar,
    threshold,
    source_field,
    target_fields=None,
    overwrite_fields=False,
    new_fields_complement_name="f",
    # logger_name="__name__",
    add_applied_filters_field=False,
):

    # logger = logging.getLogger(logger_name)

    if target_fields is None:
        target_fields = radar.fields.keys()

    if source_field in radar.fields.keys():

        for _i, field in enumerate(target_fields):

            # Se sobreescriben los campos filtrados
            if overwrite_fields:
                filtered_field = np.ma.masked_where(
                    radar.fields[source_field]["data"] > threshold, radar.fields[field]["data"]
                )

                # Agregamos el campo a los datos del radar.
                radar.add_field_like(field, field, filtered_field, replace_existing=True)

                if add_applied_filters_field:
                    if "applied_filters" in radar.fields[field].keys():
                        radar.fields[field]["applied_filters"] = (
                            radar.fields[field]["applied_filters"] + "[filt." + source_field + "]"
                        )
                    else:
                        radar.fields[field]["applied_filters"] = "[filt." + source_field + "]"

            # Generacion de Campos Filtrados
            else:
                filtered_field = np.ma.masked_where(
                    radar.fields[source_field]["data"] > threshold, radar.fields[field]["data"]
                )

                # Agregamos el campo a los datos del radar.
                radar.add_field_like(field, field + new_fields_complement_name, filtered_field, replace_existing=True)

                if add_applied_filters_field:
                    if "applied_filters" in radar.fields[field + new_fields_complement_name].keys():
                        radar.fields[field + new_fields_complement_name]["applied_filters"] = (
                            radar.fields[field]["applied_filters"] + "[filt." + source_field + "]"
                        )
                    else:
                        radar.fields[field + new_fields_complement_name]["applied_filters"] = (
                            "[filt." + source_field + "]"
                        )

    else:
        logger.error("Error en filtrado de campos. Field " + source_field + " no encontrado.")


def filter_fields_from_mask(radar, mask, target_fields=None, overwrite_fields=False, new_fields_complement_name="f"):

    # logger = logging.getLogger(logger_name)

    if target_fields is None:
        target_fields = radar.fields.keys()

    try:
        for _i, field in enumerate(target_fields):
            if field not in radar.fields.keys():
                logger.debug(field + " no existe, se saltea.")
                continue

            # Se sobreescriben los campos filtrados
            if overwrite_fields:
                filtered_field = radar.fields[field]["data"].copy()
                filtered_field.mask = mask
                radar.add_field_like(field, field, filtered_field, replace_existing=True)

            # Generacion de Campos Filtrados
            else:
                filtered_field = radar.fields[field]["data"].copy()
                filtered_field.mask = mask

                # Agregamos el campo a los datos del radar.
                radar.add_field_like(field, field + new_fields_complement_name, filtered_field, replace_existing=True)

    except Exception as e:
        logger.error("Error filtrando campos: " + str(e))


def mask_field_outside_limits(radar, radio_inf=None, radio_ext=None, az_lim1=None, az_lim2=None, fields_to_mask=None):
    """========================================================================
    Función para enmascara los gates fuera de la region determinada por
    un radio interno, un radio externo y dos azimuts.

    Attributes
    ----------
    radio_inf : int or None
        Radio límite inferior, definido en Km. Si no se provee el campo se
        fija en None y se selecciona el primer gate como limite inferior.
    radio_ext : int or None
        Radio límite exterior, definido en Km. Si no se provee el campo se
        fija en None y se selecciona el último gate como limite exterior.
    az_lim1 : int or None
        Limite azimut inferior. Si no se provee el campo se fija en None y
        se selecciona el azimut 0 como limite inferior.
    az_lim2 : int
        Limite azimut exterior. Si no se provee el campo se fija en None y
        se selecciona el último azimut como limite exterior.
    fields_to_mask : list of str or None
        Lista de campos a enmascarar. Si no se provee una lista,
        por defecto el campo se fija en None y se enmascaran todos los campos.
    #======================================================================="""
    if fields_to_mask is None:  # por defecto enmascaramos todos los fields
        fields_to_mask = radar.fields.keys()

    if az_lim1 is None:
        az_lim1 = 0

    if az_lim2 is None:
        az_lim2 = int(radar.nrays / radar.nsweeps)

    if radio_inf is None:
        gate_min = 0

    if radio_ext is None:  # se define el ultimo gate por defecto
        gate_max = radar.ngates - 1

    # determinamos los gates inf y ext que corresponden a los radios inf y ext
    if radio_inf is not None:
        gate_min = -1
        for i in range(0, radar.ngates):
            # selec el gate con una distancia levemente superior al lim fijado
            if radar.range["data"][i] >= radio_inf * 1000:
                gate_min = i

    if radio_ext is not None:
        gate_max = -1
        for i in range(gate_min, radar.ngates):
            if radar.range["data"][i] >= radio_ext * 1000:
                gate_max = i

    # Enmascaramos gates fuera de los limites seleccionados
    for field in fields_to_mask:
        for az in range(0, az_lim1):
            radar.fields[field]["data"].mask[az, :] = True

        for az in range(az_lim2, int(radar.nrays / radar.nsweeps)):
            radar.fields[field]["data"].mask[az, :] = True

        for az in range(az_lim1, az_lim2):
            for gate in range(0, radar.ngates):
                if gate <= gate_min or gate >= gate_max:
                    radar.fields[field]["data"].mask[az, gate] = True


def mask_field_inside_limits(radar, radio_inf=0, radio_ext=None, az_lim1=None, az_lim2=None, fields_to_mask=None):
    """========================================================================
    Función para enmascara los gates dentro de la region determinada por
    un radio interno, un radio externo y dos azimuts.

    Attributes
    ----------
    radio_inf : int or None
        Radio límite inferior, definido en Km. Si no se provee el campo se
        fija en None y se selecciona el primer gate como limite inferior.
    radio_ext : int or None
        Radio límite exterior, definido en Km. Si no se provee el campo se
        fija en None y se selecciona el último gate como limite exterior.
    az_lim1 : int or None
        Limite azimut inferior. Si no se provee el campo se fija en None y
        se selecciona el azimut 0 como limite inferior.
    az_lim2 : int
        Limite azimut exterior. Si no se provee el campo se fija en None y
        se selecciona el último azimut como limite exterior.
    fields_to_mask : list of str or None
        Lista de campos a enmascarar. Si no se provee una lista,
        por defecto el campo se fija en None y se enmascaran todos los campos.
    #======================================================================="""
    if fields_to_mask is None:  # por defecto enmascaramos todos los fields
        fields_to_mask = radar.fields.keys()

    if az_lim1 is None:
        az_lim1 = 0

    if az_lim2 is None:
        az_lim2 = int(radar.nrays / radar.nsweeps)

    if radio_inf is None:
        gate_min = 0

    if radio_ext is None:  # se define el ultimo gate por defecto
        gate_max = radar.ngates - 1

    # determinamos los gates inf y ext que corresponden a los radios inf y ext
    if radio_inf is not None:
        gate_min = -1
        for i in range(0, radar.ngates):
            # selec el gate con una distancia levemente superior al lim fijado
            if radar.range["data"][i] >= radio_inf * 1000:
                gate_min = i

    if radio_ext is not None:
        gate_max = -1
        for i in range(gate_min, radar.ngates):
            if radar.range["data"][i] >= radio_ext * 1000:
                gate_max = i

    # Enmascaramos gates dentro de los limites seleccionados
    for field in fields_to_mask:
        for az in range(az_lim1, az_lim2):
            radar.fields[field]["data"].mask[az, gate_min:gate_max] = True
