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

import asyncio
import capnp
from collections import defaultdict
import copy
from datetime import date, timedelta, datetime
import json
import numpy as np
import os
import pandas

from zalfmas_capnp_schemas import fbp_capnp, climate_capnp, common_capnp, soil_capnp
from zalfmas_common import common
from zalfmas_common.climate import csv_file_based
from zalfmas_services.soil import sqlite_soil_data_service as sds
import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as ps

async def run_component(port_infos_reader_sr: str, config: dict):
    #ports = await ps.PortConnector.create_from_port_infos_reader(
    #    port_infos_reader_sr, ins=["conf"], outs=["out"]
    #)
    #await ps.update_config_from_port(config, ports["conf"])

    def default_if_nan(value, default=0.0):
        return default if np.isnan(value) else value

    # read data from excel
    dfs = pandas.read_excel("/home/berg/GitHub/amei_exercises/maricopa_wheat_face/MARICOPA Wheat FACE data_2024-10-25 (ICASA data format v4.1)(PM7)(BAK1)(no soil temp).xlsx",
                            sheet_name=[
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
                            ],
                            header=2)

    # load weather data
    wstations_df = dfs["Weather_stations"]
    weather_stations = {}
    for i in wstations_df.axes[0]:
        wsid = str(wstations_df["WST_ID"][i])
        weather_stations[wsid] = {
            "WST_ID": wsid,
            "WST_LAT": float(wstations_df["WST_LAT"][i]),
            "WST_LONG": float(wstations_df["WST_LONG"][i]),
            "WST_ELEV": float(wstations_df["WST_ELEV"][i]),
            "TAV": float(wstations_df["TAV"][i]),
            "TAMP": float(wstations_df["TAMP"][i]),
            "CO2Y": float(wstations_df["CO2Y"][i]),
        }

    wdaily_df = dfs["Weather_daily"]
    weather_daily = defaultdict(lambda: {
        "start_date": None,
        "end_date": None,
        "dates": [],
        "data": defaultdict(list),
        "timeseries": {}
    })

    for ds_id in wdaily_df["WST_DATASET"].unique():
        rows_with_ds_id = wdaily_df[wdaily_df["WST_DATASET"] == ds_id]
        data = {}
        dates = list(map(lambda d: str(d)[:10], rows_with_ds_id["W_DATE"]))
        data["globrad"] = rows_with_ds_id["SRAD"].array
        data["tmax"] = rows_with_ds_id["TMAX"].array
        data["tavg"] = rows_with_ds_id["TAVD"].array
        data["tmin"] = rows_with_ds_id["TMIN"].array
        data["precip"] = rows_with_ds_id["RAIN"].array
        data["relhumid"] = rows_with_ds_id["RHAVD"].array
        data["vaporpress"] = rows_with_ds_id["VPRSD"].array
        data["wind"] = rows_with_ds_id["WIND"].array * (1.0 / 24.0 / 3.6)
        weather_daily[ds_id]["timeseries"] = csv_file_based.TimeSeries.from_dataframe(pandas.DataFrame(data=data, index=dates))

    #cap = climate_capnp.TimeSeries._new_client(weather_daily["MARA"]["timeseries"])
    #print(await cap.info())
    #print(await cap.data())

    # load soil data
    soils = defaultdict(dict)
    soil_meta_dfs = dfs["Soil_metadata"]
    for i in soil_meta_dfs.axes[0]:
        sid = str(soil_meta_dfs["SOIL_ID"][i])
        soils[sid]["profile"] = sds.Profile(
            soil_capnp.ProfileData.new_message(),
            0.0,
            0.0,
            id=sid,
        )
        soils[sid]["layers"] = {} # (SLLT, SLLB) -> dict
        soils[sid]["clayers"] = []
        soils[sid]["SLDP"] = int(soil_meta_dfs["SLDP"][i]) # cm
        #soils[sid]["SLOBS"] = int(soil_meta_dfs["SLOBS"][i])  # cm
        #soils[sid]["SLTOP"] = int(soil_meta_dfs["SLTOP"][i]) # cm
        soils[sid]["SADR"] = float(soil_meta_dfs["SADR"][i]) # 1/day
        soils[sid]["SAWC"] = int(soil_meta_dfs["SAWC"][i])  # cm
        soils[sid]["SALB"] = float(soil_meta_dfs["SALB"][i])  # cm

    soil_profiles_dfs = dfs["Soil_profile_layers"]
    for i in soil_profiles_dfs.axes[0]:
        sid = str(soil_profiles_dfs["SOIL_ID"][i])
        sllt = int(soil_profiles_dfs["SLLT"][i]) # cm
        sllb = int(soil_profiles_dfs["SLLB"][i]) # cm
        soils[sid]["layers"][(sllt, sllb)] = {
            "Thickness": [(sllb - sllt) / 100, "m"],
            "SoilOrganicCarbon": [float(default_if_nan(soil_profiles_dfs["SLOC"][i], 0.0)), "%", "% (g[C]/100g[soil])"],
            "SoilBulkDensity": [float(soil_profiles_dfs["SLBDM"][i]) * 1000, "kg m-3"],
            "FieldCapacity": [float(soil_profiles_dfs["SLDUL"][i]), "m3/m3"],
            "PoreVolume": [float(soil_profiles_dfs["SLSAT"][i]), "m3/m3"],
            "PermanentWiltingPoint": [float(soil_profiles_dfs["SLLL"][i]), "m3/m3"],
            "Clay": [float(soil_profiles_dfs["SLCLY"][i]), "%"],
            "Sand": [float(soil_profiles_dfs["SLSND"][i]), "%"],
            "pH": [float(soil_profiles_dfs["SLPHW"][i]), ""],
            "CN": [float(soil_profiles_dfs["C_N"][i]), ""],
            #"Lambda": [float(soil_profiles_dfs["SLDRL"][i]), ""],
        }

    def append_if_not_nan(l, name, value, factor=1.0):
        if np.isnan(value):
            l.append({
                "name": name,
                "f32Value": float(value) * factor
            })

    for i in soil_profiles_dfs.axes[0]:
        sid = str(soil_profiles_dfs["SOIL_ID"][i])
        sllt = int(soil_profiles_dfs["SLLT"][i]) # cm
        sllb = int(soil_profiles_dfs["SLLB"][i]) # cm
        props = []
        append_if_not_nan(props, "organicCarbon", default_if_nan(soil_profiles_dfs["SLOC"][i], 0.0))
        append_if_not_nan(props, "bulkDensity", soil_profiles_dfs["SLBDM"][i], 1000)
        append_if_not_nan(props, "fieldCapacity", soil_profiles_dfs["SLDUL"][i])
        append_if_not_nan(props, "saturation", soil_profiles_dfs["SLSAT"][i])
        append_if_not_nan(props, "permanentWiltingPoint", soil_profiles_dfs["SLLL"][i])
        append_if_not_nan(props, "clay", soil_profiles_dfs["SLCLY"][i])
        append_if_not_nan(props, "sand", soil_profiles_dfs["SLSND"][i])
        append_if_not_nan(props, "pH", soil_profiles_dfs["SLPHW"][i])
        append_if_not_nan(props, "cnRatio", soil_profiles_dfs["C_N"][i])
        append_if_not_nan(props, "soilWaterConductivityCoefficient",
        #    "f32Value": float(soil_profiles_dfs["SLDRL"][i])
        #})
        soils[sid]["clayers"].append({
            "size": (sllb - sllt) / 100.0,
            "properties": properties
        })

    for sid, data in soils.items():
        data["profile"].data.layers = data["clayers"]

    scap = soil_capnp.Profile._new_client(soils["AZMC920001"]["profile"])
    print(await scap.info())
    print(await scap.data())


    # load fields
    fields_df = dfs["Fields"]
    fields = {}
    for i in fields_df.axes[0]:
        fid = str(fields_df["FIELD_ID"][i])
        fields[fid] = {
            "FIELD_ID": fid,
            "FL_LAT": float(fields_df["FL_LAT"][i]),
            "FL_LONG": float(fields_df["FL_LONG"][i]),
            "FLELE": float(fields_df["FLELE"][i]),
            "FLSL": float(default_if_nan(fields_df["FLSL"][i])),
        }

    # load experiments
    exp_desc_df = dfs["Experiment_description"]
    experiments = defaultdict(dict)
    for i in exp_desc_df.axes[0]:
        eid = str(exp_desc_df["EID"][i])
        experiments[eid]["EID"] = eid
        #experiments[eid]["PLYR"] = int(exp_desc_df["PLYR"][i])
        #experiments[eid]["HAYR"] = int(exp_desc_df["HAYR"][i])
        experiments[eid]["treatments"] = {}

    # load treatments of experiments
    treatments_df = dfs["Treatments"]
    for i in treatments_df.axes[0]:
        eid = str(treatments_df["EID"][i])
        tid = str(treatments_df["TREAT_ID"][i])
        field_id = str(treatments_df["FIELD_ID"][i])

        experiments[eid]["treatments"][tid] = {
            "TREAT_ID": tid,
            "EID": eid,
            "field": fields[field_id],
            "WST_ID": str(treatments_df["wst_id"][i]),
            "weather_station": weather_stations.get(str(treatments_df["wst_id"][i]), None),
            "WST_DATASET": str(treatments_df["WST_DATASET"][i]),
            "weather_data": weather_daily.get(str(treatments_df["WST_DATASET"][i]), None),
            "SDAT": str(treatments_df["SDAT"][i])[:10],
            #"ENDAT": str(treatments_df["ENDAT"][i])[:10],
            "plots": {},
            "residue": {},
            "initial_conditions": None,
            "initial_condition_layers": {},
            "planting_events": {},
            "harvest_events": {},
            "tillage_events": {},
            "mulch_events": {},
            "irrigation_events": [],
            "fertilizer_events": [],
        }

    # load plots of treatments
    plots_df = dfs["Plots"]
    for i in plots_df.axes[0]:
        eid = str(plots_df["EID"][i])
        pid = str(plots_df["PLTID"][i])
        tid = str(plots_df["TREAT_ID"][i])
        sid = str(plots_df["SOIL_ID"][i])
        experiments[eid]["treatments"][tid]["plots"][pid] = {
            "PLTID": pid,
            "EID": eid,
            "TREAT_ID": tid,
            "CUL_ID": str(plots_df["CUL_ID"][i]),
            "SOIL_ID": sid,
            "soil": soils[sid],
        }

    # load treatments of experiments
    initial_df = dfs["initial_condition_layers"]
    for i in initial_df.axes[0]:
        eid = str(initial_df["EID"][i])
        tid = str(initial_df["TREAT_ID"][i])
        ictl = int(default_if_nan(initial_df["ICTL"][i], 0.0))
        icbl = int(initial_df["ICBL"][i])

        experiments[eid]["treatments"][tid]["initial_condition_layers"][(ictl, icbl)] = {
            "EID": eid,
            "TREAT_ID": tid,
            "ICDAT": str(initial_df["ICDAT"][i])[:10],
            "ICTL": ictl,
            "ICBL": icbl,
            "ICH2O": float(initial_df["ICH2O"][i]), # fraction
            "ICNH4M": float(initial_df["ICNH4M"][i]), # kg[N] ha-1
            "ICNO3M": float(initial_df["ICNO3M"][i]), # kg[N] ha-1
        }

        for p_id, p in experiments[eid]["treatments"][tid]["plots"].items():
            ls = p["soil"]["layers"]
            icl = experiments[eid]["treatments"][tid]["initial_condition_layers"]
            if (ictl, icbl) in ls and (ictl, icbl) in icl:
                ls[(ictl, icbl)]["SoilMoisturePercentFC"] = \
                    [icl[(ictl, icbl)]["ICH2O"]/ls[(ictl, icbl)]["FieldCapacity"][0]*100, "%"]
                #ls[(ictl, icbl)]["SoilAmmonium"] = [icl[(ictl, icbl)]["ICNH4M"], "kg NH4-N m-3"]
                #ls[(ictl, icbl)]["SoilNitrate"] = [icl[(ictl, icbl)]["ICNO3M"], "kg NO3-N m-3"]

    # load planting events for a treatment
    planting_df = dfs["Planting_events"]
    for i in planting_df.axes[0]:
        eid = str(planting_df["EID"][i])
        tid = str(planting_df["TREAT_ID"][i])
        experiments[eid]["treatments"][tid]["planting_events"] = {
            "PDATE": str(planting_df["PDATE"][i])[:10],
        }

    # load harvest events for a treatment
    harvest_df = dfs["Harvest_events"]
    for i in harvest_df.axes[0]:
        eid = str(harvest_df["EID"][i])
        tid = str(harvest_df["TREAT_ID"][i])
        experiments[eid]["treatments"][tid]["harvest_events"] = {
            "HADAT": str(harvest_df["HADAT"][i])[:10],
        }

    irrigation_df = dfs["Irrigation_events"]
    for i in irrigation_df.axes[0]:
        eid = str(irrigation_df["EID"][i])
        tid = str(irrigation_df["TREAT_ID"][i])
        experiments[eid]["treatments"][tid]["irrigation_events"].append({
            "IDATE": str(irrigation_df["IDATE"][i])[:10],
            "IROP": str(irrigation_df["IROP"][i]),
            "IRADP": int(irrigation_df["IRADP"][i]), #cm
            "IRVAL": float(irrigation_df["IRVAL"][i]),
            "IRNPC": float(irrigation_df["IRNPC"][i]),
        })

    fertilizer_df = dfs["Fertilizer_events"]
    for i in fertilizer_df.axes[0]:
        eid = str(fertilizer_df["EID"][i])
        tid = str(fertilizer_df["TREAT_ID"][i])
        experiments[eid]["treatments"][tid]["fertilizer_events"].append({
            "FEDATE": str(fertilizer_df["FEDATE"][i])[:10],
            "FEACD": str(fertilizer_df["FEACD"][i]),
            "FEDEP": int(fertilizer_df["FEDEP"][i]),  # cm
            "FECD": str(fertilizer_df["FECD"][i]),
            "FEAMN": float(default_if_nan(fertilizer_df["FEAMN"][i])),
            "FENO3": float(default_if_nan(fertilizer_df["FENO3"][i])),
            "FENH4": float(default_if_nan(fertilizer_df["FENH4"][i])),
        })

    residues_df = dfs["Residue"]
    for i in residues_df.axes[0]:
        eid = str(residues_df["EID"][i])
        tid = str(residues_df["TREAT_ID"][i])
        icrdp = residues_df["ICRDP"][i]
        perc_incorp = residues_df["ICRIP"][i]
        above_ground = residues_df["ICRAG"][i]
        perc_n_conc = residues_df["ICRN"][i]
        root_wt_prev_crop = residues_df["ICRT"][i]
        experiments[eid]["treatments"][tid]["residue"] = {
            "EID": eid,
            "TREAT_ID": tid,
            "ICRDAT": str(residues_df["ICRDAT"][i])[:10],
            "ICRDP": float(icrdp) if np.isnan(icrdp) else None, # cm depth
            "ICPCR": str(residues_df["ICPCR"][i]), # residue_prev_crop #code
            "ICRIP": float(perc_incorp), # % incorporated
            "ICRAG": float(above_ground), # kg[dDM] ha-1
            "ICRN": float(perc_n_conc), # % N
            "ICRT": float(root_wt_prev_crop), # kg[DM] ha-1
        }


    while ports["out"]:
        try:
            # loop over all the experiments
            for e_id, e in experiments.items():
                for t_id, t in e["treatments"].items():
                    for p_id, p in t["plots"].items():

                        content = ""
                        out_ip = fbp_capnp.IP.new_message(content=content)
                        #common.copy_and_set_fbp_attrs(in_ip, out_ip, **{config["to_attr"]: attr})
                        await ports["out"].write(value=out_ip)

        except capnp.KjException as e:
            print(
                f"{os.path.basename(__file__)}: {config['name']} RPC Exception:",
                e.description,
            )

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")

default_config = {
    "port:conf": "[TOML string] -> component configuration",
    "port:out": "[json] -> json object describing a single plots data",
}


def main():
    parser = c.create_default_fbp_component_args_parser("Read AgMIP file")
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()



                #
                # env_template["params"]["siteParameters"]["SoilProfileParameters"] = list(map(lambda k_v: k_v[1], p["soil"]["layers"].items()))
                # env_template["params"]["siteParameters"]["Latitude"] = float(t["field"]["FL_LAT"])
                # env_template["params"]["siteParameters"]["HeightNN"] = float(t["field"]["FLELE"])
                # env_template["params"]["siteParameters"]["Slope"] = float(t["field"]["FLSL"])
                # env_template["params"]["userEnvironmentParameters"]["Albedo"] = float(p["soil"]["SALB"])
                # env_template["params"]["userEnvironmentParameters"]["AtmosphericCO2"] = float(t["weather_station"]["CO2Y"])
                #
                # env_template["cropRotation"][0]["worksteps"][0]["date"] = t["planting_events"]["PDATE"]
                # env_template["cropRotation"][0]["worksteps"][1]["date"] = t["harvest_events"]["HADAT"]
                #
                # #with open("climate-iso.csv", "r") as _:
                # #    csv_str = _.read()
                # #env_template["climateCSV"] = csv_str
                #
                # env_template["climateData"] = {
                #     "startDate": t["SDAT"], #t["weather_data"]["start_date"],
                #     "endDate": f"{t['harvest_events']['HADAT'][:4]}-12-31", #t["weather_data"]["end_date"],
                #     "data": t["weather_data"]["data"],
                #     "tamp": float(t["weather_station"]["TAMP"]),
                #     "tav": float(t["weather_station"]["TAV"]),
                # }
                #
                # irr_fert_evs = defaultdict(list)
                # for e in t["fertilizer_events"]:
                #     irr_fert_evs[e["FEDATE"]].append(e)
                # for e in t["irrigation_events"]:
                #     irr_fert_evs[e["IDATE"]].append(e)
                #
                # irr_fert_dates = list(irr_fert_evs.keys())
                # irr_fert_dates.sort()
                #
                # sowing_date = env_template["cropRotation"][0]["worksteps"][0]["date"]
                # harvest_date = env_template["cropRotation"][0]["worksteps"][-1]["date"]
                # for if_date in irr_fert_dates:
                #     kg_n_per_ha_nitrate_in_irr_water = None
                #     for ev in irr_fert_evs[if_date]:
                #         if "FEDATE" in ev:
                #             if ev["FEACD"] == "Applied in irrigation water":
                #                 kg_n_per_ha_nitrate_in_irr_water = ev["FEAMN"]
                #                 continue
                #             mf = copy.deepcopy(crop_json["ws"]["MineralFertilization"])
                #             mf["date"] = ev["FEDATE"]
                #             mf["amount"][0] = ev["FEAMN"]
                #             mf["partition"] = {
                #                 "Carbamid": 100.0,
                #                 "NH4": 0.0,
                #                 "NO3": 0.0,
                #                 "name": ev["FECD"],
                #             }
                #             if mf["date"] < sowing_date:
                #                 env_template["cropRotation"][0]["worksteps"].insert(0, mf)
                #             elif mf["date"] > harvest_date:
                #                 env_template["cropRotation"][0]["worksteps"].append(mf)
                #             else:
                #                 env_template["cropRotation"][0]["worksteps"].insert(-1, mf)
                #         elif "IDATE" in ev:
                #             irr = copy.deepcopy(crop_json["ws"]["Irrigation"])
                #             irr["date"] = ev["IDATE"]
                #             layer_size_cm = env_template["params"]["siteParameters"]["LayerThickness"][0] * 100.0 # m -> cm
                #             irr["atLayer"] = int(ev["IRADP"] / layer_size_cm)  # into which layer
                #             irr["amount"][0] = ev["IRVAL"]
                #             if kg_n_per_ha_nitrate_in_irr_water:
                #                 irr["parameters"]["nitrateConcentration"] = kg_n_per_ha_nitrate_in_irr_water * 100.0 / ev["IRVAL"] # kg/ha -> mg/l (mg/dm3)
                #             env_template["cropRotation"][0]["worksteps"].insert(-1, irr)
                #
