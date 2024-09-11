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
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import capnp
from collections import defaultdict
from datetime import date, timedelta, datetime
import json
import os
import pandas
from pathlib import Path
import sys
import time
import zmq

import monica_run_lib

from zalfmas_common import common
from zalfmas_common.model import monica_io
import zalfmas_capnpschemas

sys.path.append(os.path.dirname(zalfmas_capnpschemas.__file__))
import fbp_capnp

PATHS = {
    # adjust the local path to your environment
    "mbm-local-local": {
        "monica-path-to-climate-dir": "/home/berg/GitHub/amei_monica_soil_temperature_sensitivity_analysis/input_data/WeatherData/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-debug-write-folder": "./debug-out/",
    },
    "mbm-win-local-local": {
        "monica-path-to-climate-dir": "C:/Users/berg/GitHub/amei_monica_soil_temperature_sensitivity_analysis/input_data/WeatherData/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-debug-write-folder": "./debug-out/",
    },
    "mbm-local-remote": {
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-debug-write-folder": "./debug-out/",
    },
    "hpc-local-remote": {
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-debug-write-folder": "./debug-out/",
    },
}


def run_producer(server=None, port=None):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)  # pylint: disable=no-member

    config = {
        "mode": "mbm-win-local-local",
        "server-port": port if port else "6666",
        "server": server if server else "localhost",  # "login01.cluster.zalf.de",
        "sim.json": "sim.json",
        "crop.json": "crop.json",
        "site.json": "site.json",
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    # select paths
    paths = PATHS[config["mode"]]
    # connect to monica proxy (if local, it will try to connect to a locally started monica)
    socket.connect("tcp://" + config["server"] + ":" + str(config["server-port"]))

    # read data from excel
    dfs = pandas.read_excel("AMEI_fallow_Aimes_2024-05-16.xlsx",
                            sheet_name=[
                                "Experiment_description",
                                "Fields",
                                "Treatments",
                                "Plots",
                                "Residue",
                                "initial_condition_layers",
                                "Planting_events",
                                "Harvest_events",
                                "Soil_metadata",
                                "Soil_profile_layers",
                                "Weather_stations",
                                "Weather_daily",
                            ],
                            header=2,
                            )

    soils = defaultdict(dict)
    soil_meta_dfs = dfs["Soil_metadata"]
    for i in soil_meta_dfs.axes[0]:
        sid = soil_meta_dfs["SOIL_ID"][i]
        soils[sid]["layers"] = {} # (SLLT, SLLB) -> dict
        soils[sid]["SLDP"] = soil_meta_dfs["SLDP"][i] # cm
        soils[sid]["SLOBS"] = soil_meta_dfs["SLOBS"][i]  # cm
        soils[sid]["SLTOP"] = soil_meta_dfs["SLTOP"][i] # cm
        soils[sid]["SADR"] = soil_meta_dfs["SADR"][i] # 1/day
        soils[sid]["SAWC"] = soil_meta_dfs["SAWC"][i]  # cm
        soils[sid]["SALB"] = soil_meta_dfs["SALB"][i]  # cm

    soil_profiles_dfs = dfs["Soil_profile_layers"]
    for i in soil_profiles_dfs.axes[0]:
        sid = soil_profiles_dfs["SOIL_ID"][i]
        sllt_str = soil_profiles_dfs["SLLT"][i]
        sllt = float(sllt_str) if sllt_str else 0
        sllb_str = soil_profiles_dfs["SLLB"][i]
        sllb = float(sllb_str) if sllb_str else 0
        soils[sid]["layers"][(sllt, sllb)] = {
            "Thickness": [(sllb - sllt) / 100, "m"],
            "SoilOrganicCarbon": [float(soil_profiles_dfs["SLOC"][i]), "% (g[C]/100g[soil])"],
            "SoilBulkDensity": [float(soil_profiles_dfs["SLBDM"][i]) * 1000, "kg m-3"],
            "FieldCapacity": [float(soil_profiles_dfs["SLDUL"][i]), "m3/m3"],
            "PoreVolume": [float(soil_profiles_dfs["SLSAT"][i]), "m3/m3"],
            "PermanentWiltingPoint": [float(soil_profiles_dfs["SLLL"][i]), "m3/m3"],
            "Clay": [float(soil_profiles_dfs["SLCLY"][i]), "%"],
            "Sand": [float(soil_profiles_dfs["SLSND"][i]), "%"],
            "PH": [float(soil_profiles_dfs["SLPHW"][i]), ""],
            "CN": [float(soil_profiles_dfs["SLCN"][i]), ""],
            # "Lambda": [float(sps["SLDRL"][i]), ""],
        }

    # load fields
    fields_df = dfs["Fields"]
    fields = {}
    for i in fields_df.axes[0]:
        fid = fields_df["FIELD_ID"][i]
        fields["FIELD_ID"] = fid
        fields["FL_LAT"] = fields_df["FL_LAT"][i]
        fields["FL_LONG"] = fields_df["FL_LONG"][i]
        fields["FLELE"] = fields_df["FLELE"][i]
        fields["FLSL"] = fields_df["FLSL"][i]

    # load experiments
    exp_desc_df = dfs["Experiment_description"]
    experiments = defaultdict(dict)
    for i in exp_desc_df.axes[0]:
        eid = exp_desc_df["EID"][i]
        experiments[eid]["EID"] = eid
        experiments[eid]["PLYR"] = exp_desc_df["PLYR"][i]
        experiments[eid]["HAYR"] = exp_desc_df["HAYR"][i]
        experiments[eid]["treatments"] = {}

    # load treatments of experiments
    treatments_df = dfs["Treatments"]
    for i in treatments_df.axes[0]:
        eid = treatments_df["EID"][i]
        tid = treatments_df["TREAT_ID"][i]
        field_id = treatments_df["FIELD_ID"][i]

        experiments[eid]["treatments"][tid] = {
            "TREAT_ID": tid,
            "EID": eid,
            "field": fields[field_id],
            "wst_id": treatments_df["wst_id"][i],
            "WST_DATASET": treatments_df["WST_DATASET"][i],
            "SDAT": datetime.strptime(treatments_df["SDAT"][i], "%Y.%m.%d").isoformat(),
            "ENDAT": datetime.strptime(treatments_df["ENDAT"][i], "%Y.%m.%d").isoformat(),
            "plots": {},
            "residue": {},
            "initial_conditions": {},
        }

    # load treatments of experiments
    initial_df = dfs["initial_conditions_layer"]
    for i in initial_df.axes[0]:
        eid = initial_df["EID"][i]
        tid = initial_df["TREAT_ID"][i]
        ictl = initial_df["ICTL"][i]
        icbl = initial_df["ICBL"][i]

        experiments[eid]["treatments"][tid]["initial_conditions"][(ictl, icbl)] = {
            "EID": eid,
            "TREAT_ID": tid,
            "ICDAT": datetime.strptime(initial_df["ICDAT"][i], "%Y.%m.%d").isoformat(),
            "ICTL": ictl,
            "ICBL": icbl,
            "ICH2O": float(initial_df["ICH2O"][i]), # fraction
            "ICNH4M": float(initial_df["ICNH4M"][i]), # kg[N] ha-1
            "ICNO3M": float(initial_df["ICNO3M"][i]), # kg[N] ha-1
        }

    # load planting events for a treatment
    planting_df = dfs["Planting_events"]
    for i in planting_df.axes[0]:
        eid = planting_df["EID"][i]
        tid = planting_df["TREAT_ID"][i]
        experiments[eid]["treatments"][tid]["planting"] = {
            "PDATE": datetime.strptime(treatments_df["PDATE"][i], "%Y.%m.%d").isoformat(),
        }

    # load harvest events for a treatment
    harvest_df = dfs["Harvest_events"]
    for i in harvest_df.axes[0]:
        eid = harvest_df["EID"][i]
        tid = harvest_df["TREAT_ID"][i]
        experiments[eid]["treatments"][tid]["harvest"] = {
            "HADAT": datetime.strptime(treatments_df["PDATE"][i], "%Y.%m.%d").isoformat(),
        }

    residues_df = dfs["Residue"]
    for i in residues_df.axes[0]:
        eid = treatments_df["EID"][i]
        tid = treatments_df["TREAT_ID"][i]
        residue_prev_crop = residues_df["ICRCR"][i]
        perc_incorp = treatments_df["ICRIP"][i]
        depth_cm = treatments_df["ICRDP"][i]
        above_ground = treatments_df["ICRAG"][i]
        perc_n_conc = treatments_df["ICRN"][i]
        root_wt_prev_crop = treatments_df["ICRT"][i]
        experiments[eid]["treatments"][tid]["residue"] = {
            "EID": eid,
            "TREAT_ID": tid,
            "ICRDAT": datetime.strptime(treatments_df["ICRDAT"][i], "%Y.%m.%d").isoformat(),
            "ICRDP": float(depth_cm) if depth_cm else None, # cm depth
            "ICRCR": float(residue_prev_crop), # code
            "ICRIP": float(perc_incorp), # % incorporated
            "ICRAG": float(above_ground), # kg[dDM] ha-1
            "ICRN": float(perc_n_conc), # % N
            "ICRT": float(root_wt_prev_crop), # kg[DM] ha-1
        }

    # load plots of treatments
    plots_df = dfs["Plots"]
    for i in plots_df.axes[0]:
        eid = plots_df["EID"][i],
        pid = plots_df["PLTID"][i]
        tid = plots_df["TREAT_ID"][i]
        sid = plots_df["SOIL_ID"][i]
        experiments[eid]["treatments"][tid]["plots"][pid] = {
            "PLTID": pid,
            "EID": eid,
            "TREAT_ID": tid,
            "CUL_ID": plots_df["CUL_ID"][i],
            "SOIL_ID": sid,
            "soil": soils[sid],
        }

    # load weather data
    wstations_df = dfs["Weather_stations"]
    weather_stations = {}
    for i in wstations_df.axes[0]:
        wsid = wstations_df["WST_ID"][i],
        weather_stations[wsid] = {
            "WST_ID": wsid,
            "WST_LAT": float(wstations_df["WST_LAT"][i]),
            "WST_LONG": float(wstations_df["WST_LONG"][i]),
            "WST_ELE": float(wstations_df["WST_ELE"][i]),
            "WST_TAV": float(wstations_df["WST_TAV"][i]),
            "WST_TAMP": float(wstations_df["WST_TAMP"][i]),
            "CO2Y": float(wstations_df["CO2Y"][i]),
        }

    wdaily_df = dfs["Weather_daily"]
    weather_daily = defaultdict(lambda: {
        "startDate": None,
        "endDate": None,
        "dates": [],
        "data": defaultdict(list)
    })
    for i in wdaily_df.axes[0]:
        ds_id = wdaily_df["WST_DATASET"][i],
        date = datetime.strptime(wdaily_df["W_DATE"][i], "%Y.%m.%d").isoformat(),
        weather_daily[ds_id]["dates"].append(date)
        weather_daily[ds_id]["data"][8].append(float(wdaily_df["SRAD"][i])) # globrad MJ m-2 day-1
        weather_daily[ds_id]["data"][5].append(float(wdaily_df["TMAX"][i])) # max temp °C
        weather_daily[ds_id]["data"][4].append(float(wdaily_df["TAVD"][i]))  # tavg temp °C
        weather_daily[ds_id]["data"][3].append(float(wdaily_df["TMIN"][i])) # min temp °C
        weather_daily[ds_id]["data"][6].append(float(wdaily_df["RAIN"][i])) # precip mm
        weather_daily[ds_id]["data"][14].append(float(wdaily_df["VPRSD"][i]))  # kPa
        weather_daily[ds_id]["data"][9].append(float(wdaily_df["WIND"][i]) / 24 / 3.6) # wind km/day -> m/s

    for ds_id, data in weather_daily.items():
        data["startDate"] = data["dates"][0]
        data["endDate"] = data["dates"][-1]

    # read template sim.json
    with open(config["sim.json"]) as _:
        sim_json = json.load(_)
    # read template site.json
    with open(config["site.json"]) as _:
        site_json = json.load(_)
    # read template crop.json
    with open(config["crop.json"]) as _:
        crop_json = json.load(_)
    # create environment template from json templates
    env_template = monica_io.create_env_json_from_json_config({
        "crop": crop_json,
        "site": site_json,
        "sim": sim_json,
        "climate": ""
    })

    exp_desc_df = dfs["Experiment_description"]
    fields = dfs["Fields"]
    treatments_df = dfs["Treatments"]
    # loop over all the experiments
    for e in exp_desc_df.axes[0]:
        eid = exp_desc_df["EID"][e]
        year = exp_desc_df["PLYR"][e]

        for t in treatments_df.axes[0]:
            tid = treatments_df["TREAT_ID"][t]





    soil_profiles_dict = defaultdict(dict)
    soil_profiles_dfs = dfs["Soil_profile_layers"]
    for i in soil_profiles_dfs.axes[0]:
        soil_profiles_dict[soil_profiles_dfs["SOIL_ID"]][i] = {
            "Thickness": [(float(soil_profiles_dfs["SLLB"][i]) - float(soil_profiles_dfs["SLLT"][i])) / 100, "m"],
            "SoilOrganicCarbon": [float(soil_profiles_dfs["SLOC"][i]), "% (g[C]/100g[soil])"],
            "SoilBulkDensity": [float(soil_profiles_dfs["SLBDM"][i]) * 1000, "kg m-3"],
            "FieldCapacity": [float(soil_profiles_dfs["SLDUL"][i]), "m3/m3"],
            "PoreVolume": [float(soil_profiles_dfs["SLSAT"][i]), "m3/m3"],
            "PermanentWiltingPoint": [float(soil_profiles_dfs["SLLL"][i]), "m3/m3"],
            "Clay": [float(soil_profiles_dfs["SLCLY"][i]), "%"],
            "Sand": [float(soil_profiles_dfs["SLSND"][i]), "%"],
            "PH": [float(soil_profiles_dfs["SLPHW"][i]), ""],
            "CN": [float(soil_profiles_dfs["SLCN"][i]), ""],
            #"Lambda": [float(sps["SLDRL"][i]), ""],
        }
    soil_profiles = defaultdict(list)
    for soil_id, layers_dict in soil_profiles_dict.items():
        for lid in sorted(layers_dict.keys()):
            soil_profiles[soil_id].append(layers_dict[lid])



    # create set value worksteps
    icls = dfs["initial_conditions_layers"]
    initial_condition_layers_dict = defaultdict(dict)
    for i in icls.axes[0]:
        iso_date = datetime.strptime(icls["ICDAT"], "%Y.%m-%d")
        initial_condition_layers_dict[soil_profiles_dfs["ICDAT"]][i] = {
            "Thickness": [(float(soil_profiles_dfs["SLLB"]) - float(soil_profiles_dfs["SLLT"])) / 100, "m"],
            "SoilOrganicCarbon": [float(soil_profiles_dfs["SLOC"]), "% (g[C]/100g[soil])"],
            "SoilBulkDensity": [float(soil_profiles_dfs["SLBDM"]) * 1000, "kg m-3"],
            "FieldCapacity": [float(soil_profiles_dfs["SLDUL"]), "m3/m3"],
            "PoreVolume": [float(soil_profiles_dfs["SLSAT"]), "m3/m3"],
            "PermanentWiltingPoint": [float(soil_profiles_dfs["SLLL"]), "m3/m3"],
            "Clay": [float(soil_profiles_dfs["SLCLY"]), "%"],
            "Sand": [float(soil_profiles_dfs["SLSND"]), "%"],
            "PH": [float(soil_profiles_dfs["SLPHW"]), ""],
            "CN": [float(soil_profiles_dfs["SLCN"]), ""],
            # "Lambda": [float(sps["SLDRL"]), ""],
        }

    sent_env_count = 0
    start_time = time.perf_counter()
    for treatment_id, t_data in treatment_csv.items():
        start_setup_time = time.perf_counter()

        soil_id = t_data["SOIL_ID"]
        wst_id = t_data["WST_ID"]
        soil_profile = soil_profiles[soil_id]
        env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile
        env_template["params"]["siteParameters"]["Latitude"] = float(weather_metadata_csv[wst_id]["XLAT"])
        env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]
        env_template["pathToClimateCSV"] = f"{paths['monica-path-to-climate-dir']}/{t_data['WST_DATASET']}.WTH"
        # print("pathToClimateCSV:", env_template["pathToClimateCSV"])

        awc = float(t_data["AWC"])
        env_template["params"]["userSoilTemperatureParameters"]["PlantAvailableWaterContentConst"] = awc

        env_template["params"]["simulationParameters"]["customData"] = {
            "LAI": float(t_data["LAID"]),
            "AWC": awc,
            "CWAD": float(t_data["CWAD"]),
            "IRVAL": float(t_data["IRVAL"]),
            "MLTHK": float(t_data["MLTHK"]),
            "SALB": float(soil_metadata_csv[soil_id]["SALB"]),
            "SLDP": float(soil_metadata_csv[soil_id]["SLDP"]),
            "SABDM": float(soil_metadata_csv[soil_id]["SABDM"]),
            "XLAT": float(weather_metadata_csv[wst_id]["XLAT"]),
            "XLONG": float(weather_metadata_csv[wst_id]["XLONG"]),
            "TAMP": float(weather_metadata_csv[wst_id]["TAMP"]),
            "TAV": float(weather_metadata_csv[wst_id]["TAV"]),
        }

        env_template["customId"] = {
            "env_id": sent_env_count + 1,
            "location": wst_id,
            "soil": soil_id,
            "lai": f"L{t_data['LAID']}",
            "aw": f"AW{t_data['AWC']}",
            
            "layerThickness": site_json["SiteParameters"]["LayerThickness"][0],
            "profileLTs": list(map(lambda layer: layer["Thickness"][0], soil_profile))
        }

        socket.send_json(env_template)
        sent_env_count += 1

        stop_setup_time = time.perf_counter()
        print("Setup ", sent_env_count, " envs took ", (stop_setup_time - start_setup_time), " seconds")

    env_template["customId"] = {
        "no_of_sent_envs": sent_env_count,
    }
    socket.send_json(env_template)

    stop_time = time.perf_counter()

    # write summary of used json files
    try:
        print("sending ", (sent_env_count - 1), " envs took ", (stop_time - start_time), " seconds")
        print("exiting run_producer()")
    except Exception:
        raise


if __name__ == "__main__":
    run_producer()
