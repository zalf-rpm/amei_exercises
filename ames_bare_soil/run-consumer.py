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

                #st_model = custom_id["st_model"]
                model_code = custom_id["model_code"]
                year_str = custom_id["year"]
                #wst_dataset = custom_id["wst_dataset"]
                #soil_profile_id = custom_id["soil_profile_id"]

                for data in msg.get("data", []):
                    with open(f"{path_to_out}/{model_code}MOLayersAimes{year_str}.txt", "w") as _:
                        _.write(f"""\
AMEI Aimes fallow								
Model: MONICA version 3.6.36 - {datetime.now().isoformat()}							
Modeler_name: Michael Berg-Mohnicke								
			soil_layer_top_depth	soil_layer_base_depth	soil_temp_daily_avg	maximum_soil_temp_daily	minimum_soil_temp_daily	soil_water_by_layer
Framework	Model	Date	cm	cm	°C	°C	°C	cm3/cm3
(2letters)	(2letters)	(YYYY-MM-DD)	SLLT	SLLB	TSAV	TSMX	TSMN	SWLD
""")
                        results = data.get("results", [])
                        for vals in results:
                            # only store results up to 31st of October
                            if vals["Date"][5:] == "11-01":
                                break
                            for layer_index in [0, 1, 2, 3, 4, 9, 10, 18, 20]:
                                tsav_i = vals["TSAV"][layer_index]
                                tsmn = "na"
                                tsmx = "na"
                                _.write(f"MO\t{model_code}\t{vals['Date']}\t{layer_index*5}\t{(layer_index+1)*5}\t"
                                        f"{tsav_i}\t{tsmx}\t{tsmn}\t{vals['SWLD'][layer_index]}\n")

                    with open(f"{path_to_out}/{model_code}MOAimes{year_str}.txt", "w") as _:
                        _.write(f"""\
AMEI Aimes fallow									
Model: MONICA version 3.6.36 - {datetime.now().isoformat()} 									
Modeler_name: Michael Berg-Mohnicke									
			potential_evaporation	soil_evaporation_daily	potential_evapotrans	evapotranspiration_daily	ground_heat_daily	latent_heat_daily	net_radiation_daily
Framework	Model	Date	mm/d	mm/d	mm/d	mm/d	w/m2	w/m2	w/m2
(2letters)	(2letters)	(YYYY-MM-DD)	EPAD	ESAD	EOAD	ETAD	GHFD	LHFD	RHFD
""")
                        results = data.get("results", [])
                        for vals in results:
                            # only store results up to 31st of October
                            if vals["Date"][5:] == "11-01":
                                break
                            epad = "na" #vals['EPAD']
                            ghfd = "na"
                            lhfd = "na"
                            rhfd = vals['RHFD'] * (1000000.0 / 86400.0)
                            _.write(f"MO\t{model_code}\t{vals['Date']}\t{epad}\t{vals['ESAD']}\t"
                                    f"{vals['EOAD']}\t{vals['ETAD']}\t{ghfd}\t{lhfd}\t{rhfd}\n")

            if no_of_envs_expected == envs_received:
                print("last expected env received")
                leave = True

        except zmq.error.Again as _e:
            print('no response from the server (with "timeout"=%d ms) ' % socket.RCVTIMEO)
            continue
        except Exception as e:
            print("Exception:", e)
            break

    print("exiting run_consumer()")


if __name__ == "__main__":
    run_consumer()
