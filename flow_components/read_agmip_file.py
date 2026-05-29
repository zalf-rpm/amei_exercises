#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)
from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Literal, override

import capnp
import numpy as np
import pandas
import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as ps
import zalfmas_fbp.run.process as process
from pydantic import Field
from zalfmas_capnp_schemas_with_stubs import (
    climate_capnp,
    common_capnp,
    fbp_capnp,
    field_exp_data_capnp,
    soil_capnp,
)
from zalfmas_common import common
from zalfmas_common.climate import csv_file_based
from zalfmas_fbp.run import metadata as meta
from zalfmas_services.soil import sqlite_soil_data_service as sds

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s @ %(name)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

type AgmipClimateElement = Literal["SRAD", "TMAX", "TMIN", "TAVD", "RAIN", "VPRSD", "WIND", "TDEW", "RHAVD"]
type SchemaClimateElement = Literal[*climate_capnp.Element.schema.enumerants.keys()]
type AgmipExcelSheetName = Literal[
    "Experiment_description",
    "Fields",
    "Treatments",
    "Plots",
    "Residue",
    "initial_condition_layers",
    "Planting_events",
    "Harvest_events",
    "Irrigation_events",
    "Fertilizer_events",
    "Soil_metadata",
    "Soil_profile_layers",
    "Weather_stations",
    "Weather_daily",
    "Env_modifications",
    "Genotypes",
]


class CompConfig(process.ProcessConfig):
    file: str = Field(
        "/home/berg/GitHub/amei_exercises/maricopa_wheat_face/MARICOPA Wheat FACE data_2026-01-23 (ICASA data format v4.1)(PM7)(BAK1)(no soilT).xlsx",
        description="Path to AgMIP xlsx",
    )
    weather_elements: list[AgmipClimateElement] = Field(
        ["SRAD", "TMAX", "TMIN", "TAVD", "RAIN", "VPRSD", "WIND", "TDEW", "RHAVD"],
        description="Weather elements to read from file.",
    )
    enabled_sheets: list[AgmipExcelSheetName] = Field(
        [
            "Experiment_description",
            "Fields",
            "Treatments",
            "Plots",
            "Residue",
            "initial_condition_layers",
            "Planting_events",
            "Harvest_events",
            "Irrigation_events",
            "Fertilizer_events",
            "Soil_metadata",
            "Soil_profile_layers",
            "Weather_stations",
            "Weather_daily",
            "Env_modifications",
            "Genotypes",
        ],
        description="Which sheets in the xlsx file are supposed to be read and included in the output.",
    )
    agmip_elem_to_schema_elem: dict[AgmipClimateElement, list[SchemaClimateElement | float | int | list]] = Field(
        {
            "SRAD": ["globrad", 1.0],  # MJ/m2/d
            "TMAX": ["tmax", 1.0],  # °C
            "TMIN": ["tmin", 1.0],  # °C
            "TAVD": ["tavg", 1.0],  # °C
            "RAIN": ["precip", 1.0],  # mm/d
            "VPRSD": ["vaporpress", 10.0],  # kPa -> hPa
            "WIND": ["wind", [5.0, 432]],  # km/d -> m/s
            "TDEW": ["dewpointTemp", 1.0],  # °C
            "RHAVD": ["relhumid", 1.0],  # %
        },
        description="Map AgMIP climate element names to schema climate element names with conversion fractor.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="amei_exercises",
        name="AMEI Exercises",
    ),
    info=meta.Info(
        id="ba5ccca8-ecbe-4598-aa1d-0123a0a95423",
        name="Read AgMIP file",
        description="Read an AgMIP file and stream plots",
    ),
    type="process",
    inPorts=[
        meta.Port(name="conf", contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]")
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="@0xa23434cc8f8d6a77 = data/field_exp_data.capnp:MixedType",
            desc="Structure containing capabilities to the timeseries and soil profile as well as JSON data structures for the remaining data.",
        )
    ],
    config=CompConfig,
)


class Component(process.Process[CompConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        process.Process.__init__(self, metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info(f"{self.name} process running")
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        def default_if_nan(value, default: float | None = 0.0, apply_func=None):
            if value is not None:
                if type(value) is str:
                    return apply_func(value)
                try:
                    return default if np.isnan(value) else (apply_func(value) if apply_func else value)
                except:
                    return apply_func(value) if apply_func else value
            return value

        file = self.config.file
        # file = "/home/berg/GitHub/amei_exercises/maricopa_wheat_face/MARICOPA Wheat FACE data_2026-01-23 (ICASA data format v4.1)(PM7)(BAK1)(no soilT).xlsx"
        # file = "/home/berg/GitHub/amei_exercises/ames_bare_soil/AMEI_fallow_Ames_2024-05-16.xlsx"

        enabled_sheets = {
            "Experiment_description": True,
            "Fields": True,
            "Treatments": True,
            "Plots": True,
            "Residue": True,
            "initial_condition_layers": True,
            "Planting_events": True,
            "Harvest_events": True,
            "Irrigation_events": False,
            "Fertilizer_events": False,
            "Soil_metadata": True,
            "Soil_profile_layers": True,
            "Weather_stations": True,
            "Weather_daily": True,
            "Env_modifications": False,
            "Genotypes": True,
        }

        enabled_sheets.update(map(lambda k: (k, True), self.config.enabled_sheets))

        # read data from Excel file
        dfs = pandas.read_excel(
            file,
            sheet_name=list(map(lambda e2: e2[0], filter(lambda e1: e1[1], enabled_sheets.items()))),
            header=2,
        )

        # load weather data
        wstations_df = dfs["Weather_stations"]
        weather_stations = {}
        for i in wstations_df.axes[0]:
            wsid = str(wstations_df["WST_ID"][i])
            weather_stations[wsid] = {
                "WST_ID": wsid,  # [text] weather station code
                "WST_NAME": default_if_nan(
                    wstations_df.get("WST_NAME", {}).get(i, None), None, str
                ),  # [text] weather station name
                "INST_NAME": default_if_nan(
                    wstations_df.get("INST_NAME", {}).get(i, None), None, str
                ),  # [text] institute name
                "WST_SITE": default_if_nan(
                    wstations_df.get("WST_SITE", {}).get(i, None), None, str
                ),  # [text] weather station site
                "WST_LOC_1": default_if_nan(
                    wstations_df.get("WST_LOC_1", {}).get(i, None), None, str
                ),  # [text] weather station location country
                "WST_LOC_2": default_if_nan(
                    wstations_df.get("WST_LOC_2", {}).get(i, None), None, str
                ),  # [text] weather station location 2nd level
                "WST_LOC_3": default_if_nan(
                    wstations_df.get("WST_LOC_3", {}).get(i, None), None, str
                ),  # [text] weather station location 3rd level
                "WST_LAT": default_if_nan(
                    wstations_df.get("WST_LAT", {}).get(i, None), None, float
                ),  # [decimal degrees] weather station latitude
                "WST_LONG": default_if_nan(
                    wstations_df.get("WST_LONG", {}).get(i, None), None, float
                ),  # [decimal degrees] weather station longitude
                "WST_ELEV": default_if_nan(
                    wstations_df.get("WST_ELEV", {}).get(i, None), None, float
                ),  # [m] weather station elevation
                "TAV": default_if_nan(
                    wstations_df.get("TAV", {}).get(i, None), None, float
                ),  # [°C] temperature avg year
                "TAMP": default_if_nan(
                    wstations_df.get("TAMP", {}).get(i, None), None, float
                ),  # [°C] temperature amplitude month avg
                "TEMHT": default_if_nan(
                    wstations_df.get("TEMHT", {}).get(i, None), None, float
                ),  # [m] temperature sensor height
                "REFHT": default_if_nan(
                    wstations_df.get("REFHT", {}).get(i, None), None, float
                ),  # [m] reference height weather measurement
                "WNDHT": default_if_nan(
                    wstations_df.get("WNDHT", {}).get(i, None), None, float
                ),  # [m] reference height windspeed measurement
                "CO2Y": default_if_nan(
                    wstations_df.get("CO2Y", {}).get(i, None), None, float
                ),  # [ppm] CO2 concentration annual
                "WST_NOTES": default_if_nan(
                    wstations_df.get("WST_NOTES", {}).get(i, None), None, str
                ),  # [text] weather notes
            }

        agmip_elem_to_schema_elem = {
            "SRAD": ["globrad", 1.0],  # MJ/m2/d
            "TMAX": ["tmax", 1.0],  # °C
            "TMIN": ["tmin", 1.0],  # °C
            "TAVD": ["tavg", 1.0],  # °C
            "RAIN": ["precip", 1.0],  # mm/d
            "VPRSD": ["vaporpress", 10.0],  # kPa -> hPa
            "WIND": ["wind", [5.0, 432]],  # km/d -> m/s
            "TDEW": ["dewpointTemp", 1.0],  # °C
            "RHAVD": ["relhumid", 1.0],  # %
        }
        agmip_elem_to_schema_elem.update(self.config.agmip_elem_to_schema_elem)
        weather_elements = set(agmip_elem_to_schema_elem.keys())
        conf_weather_elements = self.config.weather_elements
        if len(conf_weather_elements) > 0:
            weather_elements = weather_elements.intersection(self.config.weather_elements)

        wdaily_df = dfs["Weather_daily"]
        weather_timeseries = {}
        for ds_id in wdaily_df["WST_DATASET"].unique():
            rows_with_ds_id = wdaily_df[wdaily_df["WST_DATASET"] == ds_id]
            data = {}
            dates = list(map(lambda d: str(d)[:10], rows_with_ds_id["W_DATE"]))
            for w_elem in weather_elements:
                if w_elem in agmip_elem_to_schema_elem and w_elem in rows_with_ds_id:
                    schema_elem, factor = agmip_elem_to_schema_elem[w_elem]
                    if type(factor) is list and len(factor) > 0:
                        factor = factor[0] / factor[1] if len(factor) > 1 else 1.0
                    data[schema_elem] = rows_with_ds_id[w_elem].array * factor
            weather_timeseries[ds_id] = csv_file_based.TimeSeries.from_dataframe(
                pandas.DataFrame(data=data, index=dates)
            )
        # cap = climate_capnp.TimeSeries._new_client(weather_timeseries["MARA"])
        # print(await cap.info())
        # print(await cap.data())

        # load soil data
        soils: dict[str, dict] = defaultdict(dict)
        soil_meta_dfs = dfs["Soil_metadata"]
        for i in soil_meta_dfs.axes[0]:
            sid = str(soil_meta_dfs["SOIL_ID"][i])
            soils[sid] = {
                "SOIL_ID": sid,  # [text] soil profile id
                "SOIL_NAME": default_if_nan(
                    soil_meta_dfs.get("Soil_NAME", {}).get(i, None), None, str
                ),  # [text] name of soil
                "SL_SOURCE": default_if_nan(
                    soil_meta_dfs.get("SL_SOURCE", {}).get(i, None), None, str
                ),  # [text] soil source
                "SLDP": default_if_nan(soil_meta_dfs.get("SLDP", {}).get(i, None), None, int),  # [cm] soil depth
                "SLOBS": default_if_nan(
                    soil_meta_dfs.get("SLOBS", {}).get(i, None), None, int
                ),  # [cm] soil obstacle depth
                "SLTOP": default_if_nan(
                    soil_meta_dfs.get("SLTOP", {}).get(i, None), None, int
                ),  # [cm] depth of topsoil
                "SADR": default_if_nan(
                    soil_meta_dfs.get("SADR", {}).get(i, None), None, float
                ),  # [1/day] drainage rate per day
                "SLRO": default_if_nan(
                    soil_meta_dfs.get("SLRO", {}).get(i, None), None, float
                ),  # [number] runoff curve no SCS
                "SAWC": default_if_nan(
                    soil_meta_dfs.get("SAWC", {}).get(i, None), None, float
                ),  # [cm] soil available water content
                "FLST": default_if_nan(
                    soil_meta_dfs.get("FLST", {}).get(i, None), None, float
                ),  # [m2/m2] surface stones (cover)
                "SALB": default_if_nan(soil_meta_dfs.get("SALB", {}).get(i, None), None, float),  # [] soil albedo
                "SLU1": default_if_nan(
                    soil_meta_dfs.get("SLU1", {}).get(i, None), None, float
                ),  # [mm] soil evaporation limit
                "SLNF": default_if_nan(
                    soil_meta_dfs.get("SLNF", {}).get(i, None), None, float
                ),  # [number] mineralization factor
                "SLPF": default_if_nan(
                    soil_meta_dfs.get("SLPF", {}).get(i, None), None, float
                ),  # [number] soil fertility on foto
                "SL_SYSTEM": default_if_nan(
                    soil_meta_dfs.get("SL_SYSTEM", {}).get(i, None), None, str
                ),  # [text] soil classific system
                "SLTX": default_if_nan(soil_meta_dfs.get("SLTX", {}).get(i, None), None, str),  # [code] soil texture
                "CLASSIFICATION": default_if_nan(
                    soil_meta_dfs.get("CLASSIFICATION", {}).get(i, None), None, str
                ),  # [text] soil classification
                "SL_NOTES": default_if_nan(
                    soil_meta_dfs.get("SL_NOTES", {}).get(i, None), None, str
                ),  # [text] soil notes
                "profile": sds.Profile(
                    soil_capnp.ProfileData.new_message(),
                    0.0,
                    0.0,
                    id=sid,
                ),
            }

        def append_if_not_nan(l, name, value, factor=1.0):
            if value and not np.isnan(value):
                l.append({"name": name, "f32Value": float(value) * factor})

        soil_profiles_dfs = dfs["Soil_profile_layers"]
        soil_layers = defaultdict(list)
        for i in soil_profiles_dfs.axes[0]:
            sid = str(soil_profiles_dfs["SOIL_ID"][i])
            sllt = int(soil_profiles_dfs["SLLT"][i])  # [cm] soil layer top depth
            sllb = int(soil_profiles_dfs["SLLB"][i])  # [cm] soil layer base depth
            layer_size_cm = sllb - sllt  # [cm]
            props = []
            append_if_not_nan(
                props, "saturation", soil_profiles_dfs.get("SLSAT", {}).get(i, None)
            )  # [cm3/cm3] soil water saturated
            append_if_not_nan(
                props, "fieldCapacity", soil_profiles_dfs.get("SLDUL", {}).get(i, None)
            )  # [cm3/cm3] soil water drained upper limit
            append_if_not_nan(
                props,
                "permanentWiltingPoint",
                soil_profiles_dfs.get("SLLL", {}).get(i, None),
            )  # [cm3/cm3] soil water lower limit
            # append_if_not_nan(props, "", soil_profiles_dfs.get("SLAWC", {}).get(i, None), 100.0 / layer_size_cm) # [mm -> %] soil layer available water
            # append_if_not_nan(props, "", default_if_nan(soil_profiles_dfs.get("SLRGF"][i], 0.0))
            append_if_not_nan(
                props,
                "bulkDensity",
                soil_profiles_dfs.get("SLBDM", {}).get(i, None),
                1000,
            )  # [g/cm3 -> kg/m3] soil bulk density moist
            # append_if_not_nan(props, "", soil_profiles_dfs.get("SLNI", {}).get(i, None)) # [%] soil organic N concentration
            # append_if_not_nan(props, "", soil_profiles_dfs.get("SKSAT", {}).get(i, None)) # [cm/h] saturated hydraulic conductivity
            append_if_not_nan(
                props,
                "soilWaterConductivityCoefficient",
                soil_profiles_dfs.get("SLDRL", {}).get(i, None),
            )  # [1/day] layer drainage rate per day
            append_if_not_nan(
                props,
                "organicCarbon",
                default_if_nan(soil_profiles_dfs.get("SLOC", {}).get(i, None), 0.0),
            )  # [g[C]/100g[soil]] soil organic C percent layer
            append_if_not_nan(props, "cnRatio", soil_profiles_dfs.get("C_N", {}).get(i, None))  # [-] soil CN ratio
            append_if_not_nan(
                props, "clay", soil_profiles_dfs.get("SLCLY", {}).get(i, None)
            )  # [%-wt] soil clay fraction
            append_if_not_nan(
                props, "silt", soil_profiles_dfs.get("SLSIL", {}).get(i, None)
            )  # [%-wt] soil silt fraction
            append_if_not_nan(
                props, "sand", soil_profiles_dfs.get("SLSND", {}).get(i, None)
            )  # [%-wt] soil sand fraction
            append_if_not_nan(
                props, "sceleton", soil_profiles_dfs.get("SLCF", {}).get(i, None)
            )  # [%-wt] soil coarse fraction
            append_if_not_nan(props, "pH", soil_profiles_dfs.get("SLPHW", {}).get(i, None))  # [number] soil ph in water
            # append_if_not_nan(props, "", soil_profiles_dfs.get("CACO3", {}).get(i, None)) # [g/kg] CaCO3 content
            # append_if_not_nan(props, "", soil_profiles_dfs.get("SLOM", {}).get(i, None)) # [kg[OM]/ha] soil organic matter layer
            # append_if_not_nan(props, "", soil_profiles_dfs.get("SLOMC", {}).get(i, None)) # [g[OM]/100g[soil]] soil organic matter concentration layer
            soil_layers[sid].append({"size": layer_size_cm / 100.0, "properties": props})
        for sid, layers in soil_layers.items():
            soils[sid]["profile"].data.layers = layers

        # scap = soil_capnp.Profile._new_client(soils["AZMC920001"]["profile"])
        # print(await scap.info())
        # print(await scap.data())

        # load fields
        fields_df = dfs["Fields"]
        fields = {}
        for i in fields_df.axes[0]:
            fid = str(fields_df["FIELD_ID"][i])
            fields[fid] = {
                "FIELD_ID": fid,  # [text] field id
                "FL_NAME": default_if_nan(fields_df.get("FL_NAME", {}).get(i, None), None, str),  # [text] field name
                "FL_LAT": default_if_nan(
                    fields_df.get("FL_LAT", {}).get(i, None), None, float
                ),  # [degree] field latitude
                "FL_LONG": default_if_nan(
                    fields_df.get("FL_LONG", {}).get(i, None), None, float
                ),  # [degree] field longitude
                "FLELE": default_if_nan(fields_df.get("FLELE", {}).get(i, None), None, float),  # [m] field elevation
                "FLSL": default_if_nan(
                    fields_df.get("FLSL", {}).get(i, None), None, float
                ),  # [degree angle] field slope
                "FL_DRNTYPE": default_if_nan(
                    fields_df.get("FL_DRNTYPE", {}).get(i, None), None, str
                ),  # [code] drainage type
                "WST_DIST": default_if_nan(
                    fields_df.get("WST_DIST", {}).get(i, None), None, float
                ),  # [km] weather station distance
                "FL_LOC_1": default_if_nan(
                    fields_df.get("FL_LOC_1", {}).get(i, None), None, str
                ),  # [text] field country
                "FL_LOC_2": default_if_nan(
                    fields_df.get("FL_LOC_2", {}).get(i, None), None, str
                ),  # [text] field sub country
                "FL_LOC_3": default_if_nan(
                    fields_df.get("FL_LOC_3", {}).get(i, None), None, str
                ),  # [text] field sub sub country
                "FL_NOTES": default_if_nan(fields_df.get("FL_NOTES", {}).get(i, None), None, str),  # [text] field notes
            }

        # load experiments
        exp_desc_df = dfs["Experiment_description"]
        experiments: dict[str, dict] = defaultdict(dict)
        for i in exp_desc_df.axes[0]:
            eid = str(exp_desc_df["EID"][i])
            experiments[eid] = {
                "EID": eid,  # [text] experiment id
                "SUITEID": default_if_nan(exp_desc_df.get("SUITEID", {}).get(i, None), None, str),  # [text] suite id
                "EXNAME": default_if_nan(
                    exp_desc_df.get("EXNAME", {}).get(i, None), None, str
                ),  # [text] name of experiment
                "INFRANAME": default_if_nan(
                    exp_desc_df.get("INFRANAME", {}).get(i, None), None, str
                ),  # [text] research infrastructure name
                "INNAME": default_if_nan(
                    exp_desc_df.get("INNAME", {}).get(i, None), None, str
                ),  # [text] institution name
                "RUNAME": default_if_nan(
                    exp_desc_df.get("RUNAME", {}).get(i, None), None, str
                ),  # [text] research unit name
                "FANAME": default_if_nan(
                    exp_desc_df.get("FANAME", {}).get(i, None), None, str
                ),  # [text] experimental facility name
                "SITE_NAME": default_if_nan(
                    exp_desc_df.get("SITE_NAME", {}).get(i, None), None, str
                ),  # [text] site name
                "SITE_TYPE": default_if_nan(
                    exp_desc_df.get("SITE_TYPE", {}).get(i, None), None, str
                ),  # [code] site type
                "MAIN_FACTOR": default_if_nan(
                    exp_desc_df.get("MAIN_FACTOR", {}).get(i, None), None, str
                ),  # [text] main experimental factor
                "FACTORS": default_if_nan(
                    exp_desc_df.get("FACTORS", {}).get(i, None), None, str
                ),  # [text] experimental factor comb
                "EXPER_TYPE": default_if_nan(
                    exp_desc_df.get("EXPER_TYPE", {}).get(i, None), None, str
                ),  # [code] experiment type
                "MGMT_TYPE": default_if_nan(
                    exp_desc_df.get("MGMT_TYPE", {}).get(i, None), None, str
                ),  # [code] management type
                "CR_SYSTEM": default_if_nan(
                    exp_desc_df.get("CR_SYSTEM", {}).get(i, None), None, str
                ),  # [text] cropping system
                "PLYR": default_if_nan(exp_desc_df.get("PLYR", {}).get(i, None), None, int),  # [year] planting year
                "HAYR": default_if_nan(
                    exp_desc_df.get("HAYR", {}).get(i, None), None, int
                ),  # [year] harvest operation year
                "EXP_NOTES": default_if_nan(
                    exp_desc_df.get("EXP_NOTES", {}).get(i, None), None, str
                ),  # [text] experiment notes
                "treatments": {},
            }

        # load treatments of experiments
        treatments_df = dfs["Treatments"]
        for i in treatments_df.axes[0]:
            tid = str(treatments_df["TREAT_ID"][i])
            eid = str(treatments_df["EID"][i])
            field_id = str(treatments_df["FIELD_ID"][i])
            wst_id = default_if_nan(
                treatments_df.get("WST_ID", treatments_df.get("wst_id", {}).get(i, None)),
                None,
                str,
            )
            wst_ds = default_if_nan(treatments_df.get("WST_DATASET", {}).get(i, None), None, str)

            experiments[eid]["treatments"][tid] = {
                "TREAT_ID": tid,  # [text] treatment id
                "EID": eid,  # [text] experiment id
                "FIELD_ID": field_id,  # [text] field id
                "WST_ID": wst_id,  # [text] weather station code
                "WST_DATASET": wst_ds,  # [text] weather file
                "TRT_NAME": default_if_nan(
                    treatments_df.get("TRT_NAME", {}).get(i, None), None, str
                ),  # [text] treatment name
                "SDAT": default_if_nan(
                    treatments_df.get("SDAT", {}).get(i, None),
                    None,
                    lambda v: str(v)[:10],
                ),  # [date] simulation start date
                "ENDAT": default_if_nan(
                    treatments_df.get("ENDAT", {}).get(i, None),
                    None,
                    lambda v: str(v)[:10],
                ),  # [date] simulation end date
                "IRRIG": default_if_nan(
                    treatments_df.get("IRRIG", {}).get(i, None), None, str
                ),  # [code] irrigatin applied
                "FERTILIZER": default_if_nan(
                    treatments_df.get("FERTILIZER", {}).get(i, None), None, str
                ),  # [code] fertilizer applied
                "IR": default_if_nan(treatments_df.get("IR", {}).get(i, None), None, int),  # [number] irrigation level
                "FE": default_if_nan(treatments_df.get("FE", {}).get(i, None), None, int),  # [number] fertilizer level
                "PD": default_if_nan(
                    treatments_df.get("PD", {}).get(i, None), None, int
                ),  # [number] planting date level
                "EM": default_if_nan(
                    treatments_df.get("EM", {}).get(i, None), None, int
                ),  # [number] environmental modifier level
                "IC": default_if_nan(
                    treatments_df.get("IC", {}).get(i, None), None, int
                ),  # [number] initial conditions level
                "PL": default_if_nan(
                    treatments_df.get("PL", {}).get(i, None), None, int
                ),  # [number] planting density level
                "REP_NO": default_if_nan(
                    treatments_df.get("REP_NO", {}).get(i, None), None, int
                ),  # [number] number of replicates
                "TR_NOTES": default_if_nan(
                    treatments_df.get("TR_NOTES", {}).get(i, None), None, str
                ),  # [text] treatment comment
                "field": fields[field_id],
                "weather_station": weather_stations.get(wst_id, None),
                "weather_timeseries": weather_timeseries.get(wst_ds, None),
                "plots": {},
                "residue": {},
                "initial_conditions": None,
                "initial_condition_layers": {},
                "planting_events": None,
                "harvest_events": None,
                "tillage_events": [],
                "mulch_events": [],
                "irrigation_events": [],
                "fertilizer_events": [],
                "environment_modifications": [],
            }

        cultivars = {}
        if enabled_sheets["Genotypes"]:
            genotypes_df = dfs["Genotypes"]
            for i in genotypes_df.axes[0]:
                cul_id = str(genotypes_df["CUL_ID"][i])
                cultivars[cul_id] = {
                    "CUL_ID": cul_id,  # [text] cultivar identifier
                    "CUL_NAME": default_if_nan(
                        genotypes_df.get("CUL_NAME", {}).get(i, None), None, str
                    ),  # [text] cultivar name
                    "ACCES_ID": default_if_nan(
                        genotypes_df.get("ACCES_ID", {}).get(i, None), None, str
                    ),  # [number] accession id
                    "ACCES_LOC": default_if_nan(
                        genotypes_df.get("ACCES_LOC", {}).get(i, None), None, str
                    ),  # [text] accession location
                    "CRID": default_if_nan(
                        genotypes_df.get("CRID", {}).get(i, None), None, str
                    ),  # [code] crop identifier ICASA
                    "SEED_LOT": default_if_nan(
                        genotypes_df.get("SEED_LOT", {}).get(i, None), None, str
                    ),  # [text] seed lot
                    "BREED_PRG": default_if_nan(
                        genotypes_df.get("BREED_PRG", {}).get(i, None), None, str
                    ),  # [text] breeding program
                    "CUL_ORIG": default_if_nan(
                        genotypes_df.get("CUL_ORIG", {}).get(i, None), None, str
                    ),  # [text] cultivar me orig
                    "CUL_YEAR": default_if_nan(
                        genotypes_df.get("CUL_YEAR", {}).get(i, None), None, int
                    ),  # [year] cultivar release year
                    "CUL_SYN": default_if_nan(
                        genotypes_df.get("CUL_SYN", {}).get(i, None), None, str
                    ),  # [text] cultivar synonym
                    "CUL_NOTES": default_if_nan(
                        genotypes_df.get("CUL_NOTES", {}).get(i, None), None, str
                    ),  # [text] cultivar notes
                }

        # load plots of treatments
        plots_df = dfs["Plots"]
        for i in plots_df.axes[0]:
            pid = str(plots_df["PLTID"][i])
            eid = str(plots_df["EID"][i])
            tid = str(plots_df["TREAT_ID"][i])
            cul_id = str(plots_df["CUL_ID"][i])
            sid = str(plots_df["SOIL_ID"][i])
            experiments[eid]["treatments"][tid]["plots"][pid] = {
                "PLTID": pid,  # [text] plot id
                "EID": eid,  # [text] experiment id
                "TREAT_ID": tid,  # [text] treatment id
                "CUL_ID": cul_id,  # [text] cultivar identifier
                "SOIL_ID": sid,  # [text] soil profile id
                "BLOCK": default_if_nan(plots_df.get("BLOCK", {}).get(i, None), None, int),  # [number] block number
                "PLOTno": default_if_nan(plots_df.get("PLOTno", {}).get(i, None), None, int),  # [number] plot number
                "RP": default_if_nan(plots_df.get("RP", {}).get(i, None), None, int),  # [number] replicate number
                "PLOT_X": default_if_nan(
                    plots_df.get("PLOT_X", {}).get(i, None), None, int
                ),  # [number] plot row number
                "PLOT_Y": default_if_nan(
                    plots_df.get("PLOT_Y", {}).get(i, None), None, int
                ),  # [number] plot column number
                "PLTHM": default_if_nan(
                    plots_df.get("PLOTno", {}).get(i, None), None, str
                ),  # [code] harvest method plot
                "PLOT_NOTES": default_if_nan(
                    plots_df.get("PLOT_NOTES", {}).get(i, None), None, str
                ),  # [text] plot notes
                "soil": soils[sid],
                "cultivar": cultivars.get(cul_id, None),
            }

        # load treatments of experiments
        initial_df = dfs["initial_condition_layers"]
        for i in initial_df.axes[0]:
            eid = str(initial_df["EID"][i])
            tid = str(initial_df["TREAT_ID"][i])
            ictl = default_if_nan(initial_df["ICTL"][i], 0.0, int)
            icbl = int(initial_df["ICBL"][i])
            experiments[eid]["treatments"][tid]["initial_condition_layers"][json.dumps([ictl, icbl])] = {
                "EID": eid,  # [text] experiment id
                "TREAT_ID": tid,  # [text] treatment id
                "ICDAT": str(initial_df["ICDAT"][i])[:10],  # [date] initial conditions date
                "ICTL": ictl,  # [cm] soil layer top depth
                "ICBL": icbl,  # [cm] soil layer base depth
                "ICH2O": default_if_nan(
                    initial_df.get("ICH2O", {}).get(i, None), None, float
                ),  # [mm3/mm3] initial water concentration by layer
                "ICN_TOT": default_if_nan(
                    initial_df.get("ICN_TOT", {}).get(i, None), None, float
                ),  # [kg[N]/ha] initial Ntot layer
                "ICNH4M": default_if_nan(
                    initial_df.get("ICNH4M", {}).get(i, None), None, float
                ),  # [kg[N]/ha] initial NH4 mass layer
                "ICNO3M": default_if_nan(
                    initial_df.get("ICNO3M", {}).get(i, None), None, float
                ),  # [kg[N]/ha] initial NO3 mass layer
                "ICNH4": default_if_nan(
                    initial_df.get("ICNH4", {}).get(i, None), None, float
                ),  # [ppm] initial NH4 concentration layer
                "ICNO3": default_if_nan(
                    initial_df.get("ICNO3", {}).get(i, None), None, float
                ),  # [ppm] initial NO3 concentration layer
            }

            # the initial conditions should probably be set via other means than in the soil profile directly
            # e.g. in MONICA via a dedicated workstep
            # icl = experiments[eid]["treatments"][tid]["initial_condition_layers"]
            # soil_ids = set(map(lambda v: v[1]["SOIL_ID"], filter(lambda i: i[1]["EID"] == eid and i[1]["TREAT_ID"] == tid, experiments[eid]["treatments"][tid]["plots"].items())))
            # for s_id in soil_ids:
            #    soil_layer = soil_layers.get(s_id, {}).get((ictl, icbl), None)
            #    if soil_layer:
            #        #fc = next(filter(lambda p: p["name"] == "fieldCapacity", soil_layer["properties"]))
            #        soil_layer["properties"].append({
            #            "name": "soilMoisture",
            #            "f32Value": icl[(ictl, icbl)]["ICH2O"]*100 # [mm3/mm3] -> %
            #        })
            #        soil_layer["properties"].append({
            #            "name": "ammonium",
            #            "f32Value": icl[(ictl, icbl)]["ICNH4M"]/(100.0*100.0*soil_layer["size"]) # kg[N]/ha -> kg[N]/m3
            #        })
            #        soil_layer["properties"].append({
            #            "name": "nitrate",
            #            "f32Value": icl[(ictl, icbl)]["ICNO3M"]/(100.0*100.0*soil_layer["size"]) # kg[N]/ha -> kg[N]/m3
            #        })

        # load planting events for a treatment
        if enabled_sheets["Planting_events"]:
            planting_df = dfs["Planting_events"]
            for i in planting_df.axes[0]:
                eid = str(planting_df["EID"][i])
                tid = str(planting_df["TREAT_ID"][i])
                experiments[eid]["treatments"][tid]["planting_events"] = {
                    "EID": eid,  # [text] experiment id
                    "TREAT_ID": tid,  # [text] treatment id
                    "PLDS": default_if_nan(
                        planting_df.get("PLDS", {}).get(i, None), None, str
                    ),  # [code] planting distribution
                    "PLRS": default_if_nan(planting_df.get("PLRS", {}).get(i, None), None, float),  # [cm] row spacing
                    "PLRD": default_if_nan(
                        planting_df.get("PLRD", {}).get(i, None), None, float
                    ),  # [arc degrees] row direction
                    "PLDP": default_if_nan(planting_df.get("PLDP", {}).get(i, None), None, int),  # [mm] planting depth
                    "PLLAY": default_if_nan(planting_df.get("PLLAY", {}).get(i, None), None, str),  # [text] plot layout
                    "PDATE": default_if_nan(
                        planting_df.get("PDATE", {}).get(i, None),
                        None,
                        lambda v: str(v)[:10],
                    ),  # [date] planting date
                    "PLPOP": default_if_nan(
                        planting_df.get("PLPOP", {}).get(i, None), None, int
                    ),  # [number/m2] plant population at planting
                    "APLDAE": default_if_nan(
                        planting_df.get("APLDAE", {}).get(i, None),
                        None,
                        lambda v: str(v)[:10],
                    ),  # [date] average emergence date
                    "APLPOE": default_if_nan(
                        planting_df.get("APLPOE", {}).get(i, None), None, int
                    ),  # [number/m2] average plant population at emergence
                    "PL_NOTES": default_if_nan(
                        planting_df.get("PL_NOTES", {}).get(i, None), None, str
                    ),  # [text] planting notes
                }

        # load harvest events for a treatment
        if enabled_sheets["Harvest_events"]:
            harvest_df = dfs["Harvest_events"]
            for i in harvest_df.axes[0]:
                eid = str(harvest_df["EID"][i])
                tid = str(harvest_df["TREAT_ID"][i])
                experiments[eid]["treatments"][tid]["harvest_events"] = {
                    "EID": eid,  # [text] experiment id
                    "TREAT_ID": tid,  # [text] treatment id
                    "HADAT": default_if_nan(
                        harvest_df.get("HADAT", {}).get(i, None),
                        None,
                        lambda v: str(v)[:10],
                    ),  # [date] harvest operations date
                    "HARM": default_if_nan(harvest_df.get("HARM", {}).get(i, None), None, str),  # [code] harvest method
                    "HAREA": default_if_nan(
                        harvest_df.get("HAREA", {}).get(i, None), None, float
                    ),  # [cm2] harvest area
                    "HA_NOTES": default_if_nan(
                        harvest_df.get("HA_NOTES", {}).get(i, None), None, str
                    ),  # [text] harvest notes
                    "HA_COMMENTS": default_if_nan(
                        harvest_df.get("HA_COMMENTS", {}).get(i, None), None, str
                    ),  # [text] harvest comments
                }

        if enabled_sheets["Irrigation_events"]:
            irrigation_df = dfs["Irrigation_events"]
            for i in irrigation_df.axes[0]:
                eid = str(irrigation_df["EID"][i])
                tid = str(irrigation_df["TREAT_ID"][i])
                experiments[eid]["treatments"][tid]["irrigation_events"].append(
                    {
                        "EID": eid,  # [text] experiment id
                        "TREAT_ID": tid,  # [text] treatment id
                        "IDATE": default_if_nan(
                            irrigation_df.get("IDATE", {}).get(i, None),
                            None,
                            lambda v: str(v)[:10],
                        ),  # [date] irrigation date
                        "IROP": default_if_nan(
                            irrigation_df.get("IROP", {}).get(i, None), None, str
                        ),  # [code] irrigation operation
                        "IRADP": default_if_nan(
                            irrigation_df.get("IRADP", {}).get(i, None), None, int
                        ),  # [cm] irrigation application depth
                        "IRVAL": default_if_nan(
                            irrigation_df.get("IRVAL", {}).get(i, None), None, int
                        ),  # [mm] irrigation amount
                        "IRNPC": default_if_nan(
                            irrigation_df.get("IRNPC", {}).get(i, None), None, float
                        ),  # [%] irrigation H2O N concentration
                        "IR_NOTES": default_if_nan(
                            irrigation_df.get("IR_NOTES", {}).get(i, None), None, str
                        ),  # [text] irrigation notes
                    }
                )

        if enabled_sheets["Fertilizer_events"]:
            fertilizer_df = dfs["Fertilizer_events"]
            for i in fertilizer_df.axes[0]:
                eid = str(fertilizer_df["EID"][i])
                tid = str(fertilizer_df["TREAT_ID"][i])
                experiments[eid]["treatments"][tid]["fertilizer_events"].append(
                    {
                        "EID": eid,  # [text] experiment id
                        "TREAT_ID": tid,  # [text] treatment id
                        "FEDATE": default_if_nan(
                            fertilizer_df.get("FEDATE", {}).get(i, None),
                            None,
                            lambda v: str(v)[:10],
                        ),  # [date] fertilization date
                        "FEACD": default_if_nan(
                            fertilizer_df.get("FEACD", {}).get(i, None), None, str
                        ),  # [code] fertilizer application method
                        "FEDEP": default_if_nan(
                            fertilizer_df.get("FEDEP", {}).get(i, None), None, int
                        ),  # [cm] application depth fertilizer
                        "FECD": default_if_nan(
                            fertilizer_df.get("FECD", {}).get(i, None), None, str
                        ),  # [code] fertilizer material
                        "FEAMN": default_if_nan(
                            fertilizer_df.get("FEAMN", {}).get(i, None), None, int
                        ),  # [kg[N]/ha] N in applied fertilizer
                        "FENO3": default_if_nan(
                            fertilizer_df.get("FENO3", {}).get(i, None), None, int
                        ),  # [kg[N]/ha] NO3 in applied fertilizer
                        "FENH4": default_if_nan(
                            fertilizer_df.get("FENH4", {}).get(i, None), None, int
                        ),  # [kg[N]/ha] NH4 in applied fertilizer
                        "FE_NOTES": default_if_nan(
                            fertilizer_df.get("FE_NOTES", {}).get(i, None), None, str
                        ),  # [text] fertilizer notes
                    }
                )

        if enabled_sheets["Residue"]:
            residues_df = dfs["Residue"]
            for i in residues_df.axes[0]:
                eid = str(residues_df["EID"][i])
                tid = str(residues_df["TREAT_ID"][i])
                experiments[eid]["treatments"][tid]["residue"] = {
                    "EID": eid,  # [text] experiment id
                    "TREAT_ID": tid,  # [text] treatment id
                    "ICRDAT": default_if_nan(
                        residues_df.get("ICRDAT", {}).get(i, None),
                        None,
                        lambda v: str(v)[:10],
                    ),  # [date] initial residue measure date
                    "ICRDP": default_if_nan(
                        residues_df.get("ICRDP", {}).get(i, None), None, int
                    ),  # [cm] residue incorporation depth
                    "ICRIP": default_if_nan(
                        residues_df.get("ICRIP", {}).get(i, None), None, float
                    ),  # [%] residue incorporated
                    "ICPCR": default_if_nan(
                        residues_df.get("ICPCR", {}).get(i, None), None, str
                    ),  # [code] residue nature prev crop
                    "ICRAG": default_if_nan(
                        residues_df.get("ICRAG", {}).get(i, None), None, float
                    ),  # [kg[dry matter]/ha] residue above ground weight
                    "ICRN": default_if_nan(
                        residues_df.get("ICRN", {}).get(i, None), None, float
                    ),  # [%] residue N concentration
                    "ICRT": default_if_nan(
                        residues_df.get("ICRT", {}).get(i, None), None, float
                    ),  # [kg[dry matter]/ha] root weight previous crop
                }

        if enabled_sheets["Env_modifications"]:
            env_mods_df = dfs["Env_modifications"]
            for i in env_mods_df.axes[0]:
                eid = str(env_mods_df["EID"][i])
                tid = str(env_mods_df["TREAT_ID"][i])
                cur_mod = {
                    "EID": eid,  # [text] experiment id
                    "TREAT_ID": tid,  # [text] treatment id
                    "EMDATE": default_if_nan(
                        env_mods_df.get("EMDATE", {}).get(i, None),
                        None,
                        lambda v: str(v)[:10],
                    ),  # [date] environment modification date
                    "ECCO2": default_if_nan(
                        env_mods_df.get("ECCO2", {}).get(i, None), None, str
                    ),  # [code] environment modification code CO2
                    "EMCO2": default_if_nan(
                        env_mods_df.get("EMCO2", {}).get(i, None), None, int
                    ),  # [ppm] environment modification CO2
                    "EM_NOTES": default_if_nan(
                        env_mods_df.get("EM_NOTES", {}).get(i, None), None, str
                    ),  # [text] environment modification notes
                }
                experiments[eid]["treatments"][tid]["environment_modifications"].append(cur_mod)

            for _, e in experiments.items():
                for _, t in e["treatments"].items():
                    weather_timeseries = t["weather_timeseries"] = csv_file_based.TimeSeries.from_dataframe(
                        t["weather_timeseries"].dataframe.copy()
                    )
                    for cur_mod in t["environment_modifications"]:
                        ts_df = weather_timeseries.dataframe
                        if "co2" not in ts_df:
                            if not (cur_default_co2 := t["weather_station"].get("CO2Y", 370)):
                                cur_default_co2 = 370
                            ts_df["co2"] = cur_default_co2
                        if cur_mod["ECCO2"] == "Replace" and cur_mod["EMCO2"]:
                            ts_df.loc[cur_mod["EMDATE"] :, "co2"] = float(cur_mod["EMCO2"])
                        elif cur_mod["ECCO2"] == "Add" and cur_mod["EMCO2"]:
                            ts_df.loc[cur_mod["EMDATE"] :, "co2"] += float(cur_mod["EMCO2"])

        if self.out_ports["out"]:
            try:
                # loop over all the experiments
                for e_id, e in experiments.items():
                    if self.stopping:
                        break

                    for t_id, t in e["treatments"].items():
                        if self.stopping:
                            break

                        for p_id, p in t["plots"].items():
                            if self.stopping:
                                break

                            content = field_exp_data_capnp.MixedType.new_message(
                                soilProfile=p["soil"]["profile"],
                                soil=common_capnp.StructuredText.new_message(
                                    value=json.dumps(p["soil"] | {"profile": None}),
                                    type="json",
                                ),
                                plot=common_capnp.StructuredText.new_message(
                                    value=json.dumps(p | {"soil": None}), type="json"
                                ),
                                timeseries=t["weather_timeseries"],
                                treatment=common_capnp.StructuredText.new_message(
                                    value=json.dumps(t | {"weather_timeseries": None, "plots": None}),
                                    type="json",
                                ),
                                experiment=common_capnp.StructuredText.new_message(
                                    value=json.dumps(e | {"treatments": None}),
                                    type="json",
                                ),
                            )

                            out_ip = fbp_capnp.IP.new_message(content=content)
                            # common.copy_and_set_fbp_attrs(in_ip, out_ip, **{self.config.to_attr: attr})
                            if not await self.write_out("out", out_ip):
                                logger.info("%s: process finished", self.name)
                                return

            except capnp.KjException as e:
                logger.exception("%s: RPC Exception: %s", self.name, e.description())

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
