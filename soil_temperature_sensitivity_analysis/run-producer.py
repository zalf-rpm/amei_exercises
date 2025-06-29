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

from collections import defaultdict
from datetime import date, timedelta, datetime
import json
import os
from pathlib import Path
import sys
import time
import zmq
from zalfmas_common import common, csv
from zalfmas_common.model import monica_io

PATHS = {
    # adjust the local path to your environment
    "mbm-local-local": {
        "monica-path-to-climate-dir": "/home/berg/GitHub/amei_exercises/soil_temperature_sensitivity_analysis/input_data/WeatherData/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-debug-write-folder": "./debug-out/",
    },
    "mbm-win-local-local": {
        "monica-path-to-climate-dir": "C:/Users/berg/GitHub/amei_exercises/soil_temperature_sensitivity_analysis/input_data/WeatherData/",
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
        "mode": "mbm-local-local",
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

    soil_data_csv = csv.read_csv("input_data/SoilData.csv",
                                            key=("SOIL_ID", "SLID"), key_type=(str, int),
                                            header_row_line=3, data_row_start=4)

    soil_metadata_csv = csv.read_csv("input_data/SoilMetadata.csv",
                                            key="SOIL_ID", key_type=(str,),
                                            header_row_line=3, data_row_start=4)


    treatment_csv = csv.read_csv("input_data/Treatment.csv",
                                            key="SM", key_type=(str,),
                                            header_row_line=3, data_row_start=4)

    weather_metadata_csv = csv.read_csv("input_data/WeatherMetadata.csv",
                                                   key="WST_ID", key_type=(str,),
                                                   header_row_line=3, data_row_start=4)

    soil_profiles_dict = defaultdict(dict)
    for (soil_id, layer_id), soil_data in soil_data_csv.items():
        soil_profiles_dict[soil_id][layer_id] = {
            "Thickness": [float(soil_data["THICK"]) / 100, "m"],
            "SoilOrganicCarbon": [float(soil_data["SLOC"]), "% (g[C]/100g[soil])"],
            "SoilBulkDensity": [float(soil_data["SLBDM"]) * 1000, "kg m-3"],
            "FieldCapacity": [float(soil_data["SLDUL"]), "m3/m3"],
            "PoreVolume": [float(soil_data["SLSAT"]), "m3/m3"],
            #"PoreVolume": [float(soil_data["SLSAT"])+0.1, "m3/m3"],
            "PermanentWiltingPoint": [float(soil_data["SLLL"]), "m3/m3"],
            "Clay": [float(soil_data["SLCLY"]), "%"],
            "Sand": [float(soil_data["SLSND"]), "%"],
            "Silt": [float(soil_data["SLSIL"]), "%"],
        }
    soil_profiles = defaultdict(list)
    for soil_id, layers_dict in soil_profiles_dict.items():
        for lid in sorted(layers_dict.keys()):
            soil_profiles[soil_id].append(layers_dict[lid])

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

        #env_template["params"]["userEnvironmentParameters"]["Albedo"] = float(soil_metadata_csv[soil_id]["SALB"])
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

        #if wst_id != "CAQC" or soil_id != "SALO" or int(t_data['LAID']) != 0 or int(float(t_data['AWC'])*100) != 0:
        #if wst_id != "FRLU" or soil_id != "SILO" or int(t_data['LAID']) != 7 or int(float(t_data['AWC'])*100) != 75:
        #if wst_id != "USMA" or soil_id != "SILO" or int(t_data['LAID']) != 7 or int(float(t_data['AWC'])*100) != 0:
        #   continue

        env_template["customId"] = {
            "env_id": sent_env_count + 1,
            "location": wst_id,
            "soil": soil_id,
            "lai": f"L{t_data['LAID']}",
            "aw": f"AW{t_data['AWC']}",
            
            "layerThickness": site_json["SiteParameters"]["LayerThickness"][0],
            "profileLTs": list(map(lambda layer: layer["Thickness"][0], soil_profile))
        }

        #with open(f"debug_out/env_{sent_env_count + 1}_{wst_id}_{soil_id}.json", "w") as _:
        #    json.dump(env_template, _, indent=2)
        socket.send_json(env_template)
        sent_env_count += 1

        stop_setup_time = time.perf_counter()
        print("Setup: ", sent_env_count, " customId: " + str(env_template["customId"]) + " took ", (stop_setup_time - start_setup_time), " seconds")

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
