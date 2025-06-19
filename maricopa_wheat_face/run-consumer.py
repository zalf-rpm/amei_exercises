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
from io import StringIO
import json
import os
import sys

import zmq
from zalfmas_common import common

def run_consumer(server=None, port=None):
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
                t_id = custom_id["treatment_id"]
                #wst_dataset = custom_id["wst_dataset"]
                #soil_profile_id = custom_id["soil_profile_id"]

                with open(f"{path_to_out}/{model_code}MOLayersMaricopa{t_id}.txt", "wt") as _:
                    _.write(f"""\
Maricopa Wheat FACE										
Model: MONICA version 3.6.38 - {datetime.now().isoformat()}
Modeler_name: Michael Berg-Mohnicke
                                    
framework_ID	model_ID	treatment_ID	date	soil_layer_top_depth	soil_layer_base_depth	soil_temp_daily_avg	maximum_soil_temp_daily	minimum_soil_temp_daily	soil_water_by_layer	soil_N_by_layer
text	text	text	(YYYY-MM-DD)	cm	cm  °C	°C	°C	cm3/cm3	kg[N]/ha
FRAMEWORK_ID	MODEL_ID	TREAT_ID	DATE	SLLT	SLLB	TSAV	TSMX	TSMN	SWLD	SNLD
""")
                    results = msg["data"][0].get("results", [])
                    for vals in results:
                        for layer_index in range(42):
                            out = StringIO()
                            out.write(f"MO\t")
                            out.write(f"{model_code}\t")
                            out.write(f"{t_id}\t")
                            out.write(f"{vals['Date']}\t")
                            out.write(f"{layer_index * 5}\t") #SLLT
                            out.write(f"{(layer_index + 1) * 5}\t") #SLLB
                            out.write(f"{vals['TSAV'][layer_index]}\t")
                            out.write("na\t") #TSMX
                            out.write("na\t") #TSMN
                            out.write(f"{vals['SWLD'][layer_index]}\t")
                            out.write(f"{vals['SNLD'][layer_index]}")
                            out.write("\n")
                            _.write(out.getvalue())

                with open(f"{path_to_out}/{model_code}MODailyMaricopa{t_id}.txt", "wt") as _:
                    _.write(f"""\
Maricopa Wheat FACE																																							
Model: MONICA version 3.6.38 - {datetime.now().isoformat()}
Modeler_name: Michael Berg-Mohnicke
                                                                                                                                                        
framework_ID	model_ID	treatment_ID	date	leaf_number_as_haun_stg	growth_stage_Zadoks	leaf_area_index	PAR_interception_daily	tops_dry_weight	grain_dry_weight	grain_unit_dry_weight	tops_N	grain_N	grain_unit_N	root_depth	soil_water_whole_profile	drainage_daily	runoff_surface	N_inorganic_day	N_leached_day	N_mineralization_day	N2O_emissions_day	N_immobilization_day	N_denitrification_day	ground_heat_daily	latent_heat_daily	sensible_heat_daily	net_radiation_daily	soil_temp_surface_daily_avg	soil_temp_surface_daily_max	soil_temp_surface_daily_min	canopy_temp_daily_avg	canopy_temp_daily_max	canopy_temp_daily_min	potential_evapotrans	evapotranspiration_daily	portential_soil_evaporation_daily	soil_evaporation_daily	potential_transpiration_daily	transpiration_daily
text	text	text	(YYYY-MM-DD)	leaf\mainstem	number	m2/m2	%	kg[DM]/ha	kg[DM]/ha	mg[DM]/grain	kg[N]/ha	kg[N]/ha	mg[N]/grain	m	cm3/cm3	mm/d	mm/d	kg[N]/ha/d	kg[N]/ha/d	kg[N]/ha/d	kg[N]/ha/d	kg[N]/ha/d	kg[N]/ha/d	w/m2	w/m2	w/m2	w/m2	°C	°C	°C	°C	°C	°C	mm/d	mm/d	mm/d	mm/d	mm/d	mm/d
FRAMEWORK_ID	MODEL_ID	TREAT_ID	DATE	LNUM	GSTZD	LAID	LIPCD	CWAD	GWAD	GWGD	CNAD	GNAD	GNGD	RDPD	SWWPD	DRND	ROFD	NIAD	NLCD	NMND	N2OED	NIMD	NDND	GHFD	LHFD	HHFD	RND	TSSAV	TSSMX	TSSMN	TGAV	TGMX	TGMN	EOAD	ETAD	EPSAD	ESAD	EPPAD	EPAD
""")
                    results = msg["data"][0].get("results", [])
                    for vals in results:
                        out = StringIO()
                        out.write("MO\t")
                        out.write(f"{model_code}\t")
                        out.write(f"{t_id}\t")
                        out.write("na\t") # LNUM = "na"
                        if vals["Stage"] == 1: out.write("0\t") #GSTZD
                        elif vals["Stage"] == 2: out.write("9\t") #GSTZD
                        elif vals["Stage"] == 3: out.write("na\t") #GSTZD
                        elif vals["Stage"] == 4: out.write("51\t") #GSTZD
                        elif vals["Stage"] == 5: out.write("65\t") #GSTZD
                        elif vals["Stage"] == 6: out.write("89\t") #GSTZD
                        out.write("na\t") #LIPCD
                        out.write("na\t") #GWGD
                        out.write(f'{vals["CNAD"]}\t')
                        out.write(f'{vals["GNAD"]}\t')
                        out.write("na\t") #GNGD
                        out.write(f'{vals["RDPD"]}\t')
                        out.write(f'{vals["SWWPD"]}\t')
                        out.write(f'{vals["DRND"]}\t')
                        out.write(f'{vals["ROFD"]}\t')
                        out.write("na\t") #NIAD
                        out.write(f'{vals["NLCD"]}\t')
                        out.write(f'{vals["NMND"]}\t')
                        out.write(f'{vals["N2OED"]}\t')
                        out.write("na\t") #NIMD
                        out.write(f'{vals["NDND"]}\t')
                        out.write("na\t") #GHFD
                        out.write("na\t") #LHFD
                        out.write("na\t") #HHFD
                        out.write(f'na\t')#{vals["RND"]}\t')
                        out.write(f'{vals["TSSAV"]}\t')
                        out.write("na\t") #TSSMX
                        out.write("na\t") #TSSMN
                        out.write("na\t") #TGAV
                        out.write("na\t") #TGMX
                        out.write("na\t") #TGMN
                        out.write(f'{vals["EOAD"]}\t')
                        out.write(f'{vals["ETAD"]}\t')
                        out.write("na\t") #EPSAD
                        out.write(f'{vals["ESAD"]}\t')
                        out.write("na\t") #EPPAD
                        out.write(f'{vals["EPAD"]}\t')
                        out.write("\n")
                        _.write(out.getvalue())

                with open(f"{path_to_out}/{model_code}MOSummaryMaricopa{t_id}.txt", "w") as _:
                    _.write(f"""\
Maricopa Wheat FACE																																		
Model: MONICA version 3.6.38 - {datetime.now().isoformat()}
Modeler_name: Michael Berg-Mohnicke
                                                                                                                                    
framework_ID	model_ID	treatment_ID	planting_date	emergence_date	anthesis_date	physiologic_maturity_dat	leaf_no_per_stem_matur	leaf_area_index_maximum	PAR_interception_over_season	tops_dry_weight_anthesis	tops_dry_weight_maturity	grain_dry_wt_at_mat	harvest_no_at_maturity	grain_unit_dry_wt_matur	tops_N_at_anthesis	tops_N_at_maturity	grain_N_at_maturity	grain_unit_N_matur	root_depth_maximum	avail_water_soil_profile_sow_mat	drainage_over_season	runoff_over_season	avail_N_inorganic_soil_profile_over_season	N_leached_during_season	N_mineralization_during_season	N2O_emissions__over_season	N_immobilization_cumul	N_denitrification_over_season	potential_evapotrans_over_season	evapotrans_over_season	potential_soil_evaporation_over_season	soil_evap_over_season	potential_transpiration_over_season	transpiration_over_season
text	text	text	date	date	date	date	leaf\mainstem	m2/m2	%	kg[DM]/ha	kg[DM]/ha	kg[DM]/ha	number/m2	mg[DM]/grain	kg[N]/ha	kg[N]/ha	kg[N]/ha	mg[N]/grain	m	mm	mm	mm	kg[N]/ha	kg[N]/ha	kg[N]/ha	kg[N]/ha	kg[N]/ha	kg[N]/ha	mm	mm	mm	mm	mm	mm
FRAMEWORK_ID	MODEL_ID	TREAT_ID	PDATE	PLDAE	ADAT	MDAT	LnoSM	LAIX	LIPCCM	CWAA	CWAM	GWAM	HnoAM	GWGM	CNAA	CNAM	GNAM	GNGM	RDPM	WAVSSM	DRCM	ROCM	NIAVSSM	NLCM	NMNCM	N2OECM	NIMCM	NDNCM	EOCM	ETCM	EPSCM	ESCM	EPPCM	EPCM
""")
                    results_summary: dict = msg["data"][1].get("results", [])
                    results_sowing = msg["data"][2].get("results", [])
                    results_emergence = msg["data"][3].get("results", [])
                    results_anthesis = msg["data"][4].get("results", [])
                    results_maturity = msg["data"][5].get("results", [])
                    for i, vals in enumerate(results_summary):
                        vals_s = results_sowing[i]
                        vals_e = results_emergence[i]
                        vals_a = results_anthesis[i]
                        vals_m = results_maturity[i]

                        out = StringIO()
                        out.write(f"MO\t")
                        out.write(f"{model_code}\t")
                        out.write(f"{t_id}\t")
                        out.write(f"{vals_s['PDATE']}\t")
                        out.write(f"{vals_e['PLDAE']}\t")
                        out.write(f"{vals_a['ADAT']}\t")
                        out.write(f"{vals_m['MDAT']}\t")
                        out.write(f"na\t") #LnoSM
                        out.write(f"{vals['LAIX']}\t")
                        out.write(f"na\t") #LIPCCM
                        out.write(f"{vals_a['CWAA']}\t")
                        out.write(f"{vals_m['CWAM']}\t")
                        out.write(f"{vals_m['GWAM']}\t")
                        out.write(f"{vals_m['HnoAM']}\t")
                        out.write(f"na\t") #GWGM
                        out.write(f"{vals_a['CNAA']}\t")
                        out.write(f"{vals_m['CNAM']}\t")
                        out.write(f"{vals_m['GNAM']}\t")
                        out.write(f"na\t") #GNGM
                        out.write(f"{vals['RDPM']}\t")
                        out.write(f"{vals['WAVSSM']}\t")
                        out.write(f"{vals['DRCM']}\t")
                        out.write(f"{vals['ROCM']}\t")
                        out.write(f"na\t") #NIAVSSM
                        out.write(f"{vals['NLCM']}\t")
                        out.write(f"{vals['NMNCM']}\t")
                        out.write(f"{vals['N2OECM']}\t")
                        out.write(f"na\t") #NIMCM
                        out.write(f"{vals['NDNCM']}\t")
                        out.write(f"{vals['EOCM']}\t")
                        out.write(f"{vals['ETCM']}\t")
                        out.write(f"na\t") #EPSCM
                        out.write(f"{vals['ESCM']}\t")
                        out.write(f"na\t") #EPPCM
                        out.write(f"{vals['EPCM']}")
                        out.write("\n")
                        _.write(out.getvalue())

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
