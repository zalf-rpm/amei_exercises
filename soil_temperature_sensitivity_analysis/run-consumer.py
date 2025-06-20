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
from datetime import datetime
import json
import os
import sys
import zmq
from zalfmas_common import common

def run_consumer(server=None, port=None):
    """collect data from workers"""

    config = {
        "mode": "remoteConsumer-remoteMonica",
        "port": port if port else "7777",
        "server": server if server else "localhost",  # "login01.cluster.zalf.de",
        "writer_sr": None,
        "path_to_out": "out",
        "timeout": 600000  # 10min
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    path_to_out = config["path_to_out"]
    if not os.path.exists(path_to_out):
        try:
            os.makedirs(path_to_out)
        except OSError:
            print("run-consumer.py: Couldn't create dir:", path_to_out, "!")

    context = zmq.Context()
    socket = context.socket(zmq.PULL)

    socket.connect("tcp://" + config["server"] + ":" + config["port"])
    socket.RCVTIMEO = config["timeout"]

    envs_received = 0
    no_of_envs_expected = None
    leave = False
    while not leave:
        try:
            msg: dict = socket.recv_json()  # encoding="latin-1"

            custom_id = msg["customId"]
            if "no_of_sent_envs" in custom_id:
                no_of_envs_expected = custom_id["no_of_sent_envs"]
            else:
                envs_received += 1
                print("received result customId:", custom_id)

                loc = custom_id["location"]
                soil = custom_id["soil"]
                lai = custom_id["lai"]
                aw = custom_id["aw"]
                lt = custom_id["layerThickness"]
                lt_cm = int(lt * 100)
                plts_cm = list(map(lambda lt_m: int(lt_m*100), custom_id["profileLTs"]))

                for data in msg.get("data", []):

                    with open(f"{path_to_out}/SoilTemperature_MO_MOO_{loc}_{soil}_{lai}_{aw}.txt", "w") as _:
                        _.write(f"DATE, SLLT, SLLB, TSLD, TSLX, TSLN\n")

                        results = data.get("results", [])
                        for vals in results:
                            _.write(f"{vals['Date']}, 0, 0, {vals['SurfTemp']}, na, na\n")
                            sum_lt_cm: int = 0
                            sum_s_temp: float = 0

                            plt_iter = iter(plts_cm)
                            plt = next(plt_iter)
                            i_plt = 1
                            for i, s_temp in enumerate(vals["SoilTemp"]):
                                sum_lt_cm += lt_cm
                                sum_s_temp += s_temp
                                if sum_lt_cm >= plt:
                                    avg_s_temp = round(sum_s_temp / (sum_lt_cm / lt_cm), 6)
                                    lower = (i + 1) * lt_cm
                                    upper = lower - sum_lt_cm
                                    _.write(f"{vals['Date']}, {upper}, {lower}, {avg_s_temp}, na, na\n")
                                    if i_plt < len(plts_cm):
                                        plt = next(plt_iter)
                                        i_plt += 1
                                    sum_lt_cm = 0
                                    sum_s_temp = 0.0

                    with open(f"{path_to_out}/SoilTemperature_MO_MOC_{loc}_{soil}_{lai}_{aw}.txt", "w") as _:
                        _.write(f"DATE, SLLT, SLLB, TSLD, TSLX, TSLN\n")
                        results = data.get("results", [])
                        for vals in results:
                            _.write(f"{vals['Date']}, 0, 0, {vals['AMEI_Monica_SurfTemp']}, na, na\n")
                            sum_lt_cm: int = 0
                            sum_s_temp: float = 0

                            plt_iter = iter(plts_cm)
                            plt = next(plt_iter)
                            i_plt = 1
                            for i, s_temp in enumerate(vals["AMEI_Monica_SoilTemp"]):
                                sum_lt_cm += lt_cm
                                sum_s_temp += s_temp
                                if sum_lt_cm >= plt:
                                    avg_s_temp = round(sum_s_temp / (sum_lt_cm / lt_cm), 6)
                                    lower = (i + 1) * lt_cm
                                    upper = lower - sum_lt_cm
                                    _.write(f"{vals['Date']}, {upper}, {lower}, {avg_s_temp}, na, na\n")
                                    if i_plt < len(plts_cm):
                                        plt = next(plt_iter)
                                        i_plt += 1
                                    sum_lt_cm = 0
                                    sum_s_temp = 0.0
                    #continue
                    with open(f"{path_to_out}/SoilTemperature_MO_DSC_{loc}_{soil}_{lai}_{aw}.txt", "w") as _:
                        _.write(f"DATE, SLLT, SLLB, TSLD, TSLX, TSLN\n")
                        results = data.get("results", [])
                        for vals in results:
                            _.write(f"{vals['Date']}, 0, 0, {vals['AMEI_DSSAT_ST_standalone_SurfTemp']}, na, na\n")
                            upper_cm = 0
                            for i, s_temp in enumerate(vals["AMEI_DSSAT_ST_standalone_SoilTemp"]):
                                lt_cm = plts_cm[i]
                                lower_cm = upper_cm + lt_cm
                                _.write(f"{vals['Date']}, {upper_cm}, {lower_cm}, {s_temp}, na, na\n")
                                upper_cm = lower_cm

                    with open(f"{path_to_out}/SoilTemperature_MO_DEC_{loc}_{soil}_{lai}_{aw}.txt", "w") as _:
                        _.write(f"DATE, SLLT, SLLB, TSLD, TSLX, TSLN\n")
                        results = data.get("results", [])
                        for vals in results:
                            _.write(f"{vals['Date']}, 0, 0, {vals['AMEI_DSSAT_EPICST_standalone_SurfTemp']}, na, na\n")
                            upper_cm = 0
                            for i, s_temp in enumerate(vals["AMEI_DSSAT_EPICST_standalone_SoilTemp"]):
                                lt_cm = plts_cm[i]
                                lower_cm = upper_cm + lt_cm
                                _.write(f"{vals['Date']}, {upper_cm}, {lower_cm}, {s_temp}, na, na\n")
                                upper_cm = lower_cm

                    with open(f"{path_to_out}/SoilTemperature_MO_SAC_{loc}_{soil}_{lai}_{aw}.txt", "w") as _:
                        _.write(f"DATE, SLLT, SLLB, TSLD, TSLX, TSLN\n")
                        results = data.get("results", [])
                        for vals in results:
                            _.write(f"{vals['Date']}, 0, 0, {vals['AMEI_Simplace_Soil_Temperature_SurfTemp']}, na, na\n")
                            upper_cm = 0
                            for i, s_temp in enumerate(vals["AMEI_Simplace_Soil_Temperature_SoilTemp"]):
                                lt_cm = plts_cm[i]
                                lower_cm = upper_cm + lt_cm
                                _.write(f"{vals['Date']}, {upper_cm}, {lower_cm}, {s_temp}, na, na\n")
                                upper_cm = lower_cm

                    with open(f"{path_to_out}/SoilTemperature_MO_SQC_{loc}_{soil}_{lai}_{aw}.txt", "w") as _:
                        _.write(f"DATE, SLLT, SLLB, TSLD, TSLX, TSLN\n")
                        results = data.get("results", [])
                        for vals in results:
                            _.write(f"{vals['Date']}, 0, 0, na, na, na\n")
                            st_min = vals["AMEI_SQ_Soil_Temperature_SoilTemp_min"]
                            st_max = vals["AMEI_SQ_Soil_Temperature_SoilTemp_max"]
                            _.write(f"{vals['Date']}, 0, 5, {round((st_min + st_max)/2.0, 6)}, {st_max}, {st_min}\n")
                            layer_depths = [(5, 15), (15, 30), (30, 45), (45, 60),
                                            (60, 90), (90, 120), (120, 150), (150, 180), (180, 210)]
                            for upper_cm, lower_cm in layer_depths:
                                _.write(f"{vals['Date']}, {upper_cm}, {lower_cm}, {vals['AMEI_SQ_Soil_Temperature_SoilTemp_deep']}, na, na\n")

                    with open(f"{path_to_out}/SoilTemperature_MO_PSC_{loc}_{soil}_{lai}_{aw}.txt", "w") as _:
                        _.write(f"DATE, SLLT, SLLB, TSLD, TSLX, TSLN\n")
                        results = data.get("results", [])
                        for vals in results:
                            _.write(
                                f"{vals['Date']}, 0, 0, {vals['AMEI_BiomaSurfacePartonSoilSWATC_SurfTemp']}, {vals['AMEI_BiomaSurfacePartonSoilSWATC_SurfTemp_max']}, {vals['AMEI_BiomaSurfacePartonSoilSWATC_SurfTemp_min']}\n")
                            upper_cm = 0
                            for i, s_temp in enumerate(vals["AMEI_BiomaSurfacePartonSoilSWATC_SoilTemp"]):
                                lt_cm = plts_cm[i]
                                lower_cm = upper_cm + lt_cm
                                _.write(f"{vals['Date']}, {upper_cm}, {lower_cm}, {s_temp}, na, na\n")
                                upper_cm = lower_cm

                    with open(f"{path_to_out}/SoilTemperature_MO_SWC_{loc}_{soil}_{lai}_{aw}.txt", "w") as _:
                        _.write(f"DATE, SLLT, SLLB, TSLD, TSLX, TSLN\n")
                        results = data.get("results", [])
                        for vals in results:
                            _.write(
                                f"{vals['Date']}, 0, 0, {vals['AMEI_BiomaSurfaceSWATSoilSWATC_SurfTemp']}, na, na\n")
                            upper_cm = 0
                            for i, s_temp in enumerate(vals["AMEI_BiomaSurfaceSWATSoilSWATC_SoilTemp"]):
                                lt_cm = plts_cm[i]
                                lower_cm = upper_cm + lt_cm
                                _.write(f"{vals['Date']}, {upper_cm}, {lower_cm}, {s_temp}, na, na\n")
                                upper_cm = lower_cm

                    with open(f"{path_to_out}/SoilTemperature_MO_STC_{loc}_{soil}_{lai}_{aw}.txt", "w") as _:
                        _.write(f"DATE, SLLT, SLLB, TSLD, TSLX, TSLN\n")
                        results = data.get("results", [])
                        for vals in results:
                            _.write(
                                f"{vals['Date']}, 0, 0, {vals['AMEI_Stics_soil_temperature_SurfTemp']}, na, na\n")
                            upper_cm = 0
                            for i, s_temp in enumerate(vals["AMEI_Stics_soil_temperature_SoilTemp"]):
                                lt_cm = plts_cm[i]
                                lower_cm = upper_cm + lt_cm
                                _.write(f"{vals['Date']}, {upper_cm}, {lower_cm}, {s_temp}, na, na\n")
                                upper_cm = lower_cm

                    with open(f"{path_to_out}/SoilTemperature_MO_APC_{loc}_{soil}_{lai}_{aw}.txt", "w") as _:
                        _.write(f"DATE, SLLT, SLLB, TSLD, TSLX, TSLN\n")
                        results = data.get("results", [])
                        for vals in results:
                            _.write(
                                f"{vals['Date']}, 0, 0, {vals['AMEI_ApsimCampbell_SurfTemp']}, {vals['AMEI_ApsimCampbell_SurfTemp_max']}, {vals['AMEI_ApsimCampbell_SurfTemp_min']}\n")
                            upper_cm = 0
                            for i, s_temp in enumerate(vals["AMEI_ApsimCampbell_SoilTemp"]):
                                lt_cm = plts_cm[i]
                                lower_cm = upper_cm + lt_cm
                                _.write(f"{vals['Date']}, {upper_cm}, {lower_cm}, {s_temp}, {vals['AMEI_ApsimCampbell_SoilTemp_max'][i]}, {vals['AMEI_ApsimCampbell_SoilTemp_min'][i]}\n")
                                upper_cm = lower_cm

            if no_of_envs_expected == envs_received:
                print("last expected env received")
                leave = True

        except zmq.error.Again as _e:
            print('no response from the server (with "timeout"=%d ms) ' % socket.RCVTIMEO)
            break
        except Exception as e:
            print("Exception:", e)
            break

    print("exiting run_consumer()")


if __name__ == "__main__":
    run_consumer()
