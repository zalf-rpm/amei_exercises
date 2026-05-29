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

import capnp
from collections import defaultdict
from datetime import date
import json
import logging
from typing import Literal, Any, override
from pydantic import Field
from zalfmas_capnp_schemas_with_stubs import fbp_capnp, field_exp_data_capnp
from zalfmas_fbp.run import metadata as meta
import zalfmas_fbp.run.process as process
from zalfmas_common import common

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s @ %(name)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class CompConfig(process.ProcessConfig):
    agmip_attr: str = Field(
        "agmip",
        description="Attribute name where to get the agmip irrigation and fertilization data from",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="amei_exercises",
        name="AMEI Exercises",
    ),
    info=meta.Info(
        id="210add72-f24e-4525-936a-d022403b7f4f",
        name="Add irr/fert data",
        description="Add irrigation and fertilization data into crop rotation",
    ),
    type="process",
    inPorts=[
        meta.Port(name="conf", contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]"),
        meta.Port(name="env", contentType="Text (JSON)"),
    ],
    outPorts=[
        meta.Port(
            name="env",
            contentType="Text (JSON)",
            desc="Updated environment with irrigation and fertilization merged into crop rotation",
        )
    ],
    config=CompConfig,
)


class AddIrrFertIntoCropRotation(process.Process[CompConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        while self.in_ports["env"] and self.out_ports["env"]:
            try:
                env_in_ip = await self.read_in("env")
                if env_in_ip is None:
                    break

                agmip = common.get_fbp_attr(env_in_ip, self.config.agmip_attr, field_exp_data_capnp.MixedType.schema)
                env = json.loads(env_in_ip.content.as_text())

                if agmip is not None and agmip._has("treatment") and agmip.treatment._has("value"):
                    t = json.loads(agmip.treatment.value)
                else:
                    continue

                worksteps = env["cropRotation"][0]["worksteps"]

                worksteps[0]["date"] = t["planting_events"]["PDATE"]
                worksteps[1]["date"] = t["harvest_events"]["HADAT"]

                kg_n_per_ha_nitrate_in_irr_water = 0.0
                for e in t.get("fertilizer_events", []):
                    if e["FEACD"] == "Applied in irrigation water":
                        kg_n_per_ha_nitrate_in_irr_water = e["FEAMN"]
                        continue
                    mf = dict(
                        type="MineralFertilization",
                        date=e["FEDATE"],
                        amount=[e["FEAMN"], "kg"],
                        partition={
                            "Carbamid": 100.0,
                            "NH4": 0.0,
                            "NO3": 0.0,
                            "name": e["FECD"],
                        },
                    )
                    worksteps.append(mf)
                for e in t.get("irrigation_events", []):
                    layer_size_cm = env["params"]["siteParameters"]["LayerThickness"][0] * 100.0  # m -> cm
                    irr = dict(
                        date=e["IDATE"],
                        type="Irrigation",
                        atLayer=int(e["IRADP"] / layer_size_cm),  # into which layer
                        amount=[e["IRVAL"], "mm"],
                        parameters={
                            "nitrateConcentration": [
                                kg_n_per_ha_nitrate_in_irr_water * 100.0 / e["IRVAL"],  # kg/ha -> mg/l (mg/dm3)
                                "mg dm-3",
                            ],
                            "sulfateConcentration": [0.0, "mg dm-3"],
                        },
                    )
                    worksteps.append(irr)
                worksteps.sort(key=lambda ws: ws["date"])

                env_out_ip = fbp_capnp.IP.new_message(content=json.dumps(env))
                common.copy_and_set_fbp_attrs(env_in_ip, env_out_ip)
                if not await self.write_out("env", env_out_ip):
                    logger.info(f"%{self.name}: process finished")
                    return

            except Exception as e:
                logger.error(f"{self.name}: Exception: {e}")

        logger.info(f"{self.name}: process finished")


def main():
    process.run_process_from_metadata_and_cmd_args(AddIrrFertIntoCropRotation(METADATA), METADATA)


if __name__ == "__main__":
    main()
