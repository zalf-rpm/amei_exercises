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

import json
import logging
from collections import defaultdict
from typing import Any, override

import zalfmas_fbp.run.process as process
from pydantic import Field
from zalfmas_capnp_schemas_with_stubs import fbp_capnp
from zalfmas_common import common
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s @ %(name)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class CompConfig(process.ProcessConfig):
    data_key: str = Field(
        "obs_crop_daily_means",
        description="Key in the incoming JSON object holding the list of observation rows.",
    )
    group_col: str = Field(
        "TREAT_ID",
        description="Column the rows are grouped by. Its values become the keys of both output objects.",
    )
    date_col: str = Field(
        "DATE",
        description="Column holding the measurement date as ISO date. Rows are ordered by it per group.",
    )
    measured_cols: list[str | list[str]] = Field(
        ["LAID", "LWAD", ["SWAD", "CRAD", "CHWAD"], "GWAD", "CWAD"],
        description=(
            "Columns making up one measured value list, in order. A list of columns instead of a single "
            "column name is summed up, where columns without a value count as 0 - unless none of them has a "
            "value, in which case the sum stays null. A row is skipped if it has no value at all."
        ),
    )
    monica_outputs: list[str | list[str]] = Field(
        ["LAI", ["OrgBiom", "Leaf"], ["OrgBiom", "Shoot"], ["OrgBiom", "Fruit"], "Yield"],
        description=(
            "The MONICA outputs to request at every date a measurement exists for. Same order as, and one "
            "output per, 'measured_cols'."
        ),
    )


METADATA = meta.Component(
    category=meta.Category(
        id="amei_exercises",
        name="AMEI Exercises",
    ),
    info=meta.Info(
        id="02d892bd-646e-466d-91ca-344e3edd43e6",
        name="Obs to calibration data",
        description=(
            "Split observation rows into the measured values to calibrate against and the MONICA output "
            "descriptions for the dates those measurements were taken at."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(name="conf", contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]"),
        meta.Port(
            name="in",
            contentType="Text (JSON)",
            desc="The observation sheet as JSON, as read by the 'Read Excel file' component.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="measured",
            contentType="Text (JSON)",
            desc=(
                "JSON object keyed by group (e.g. TREAT_ID), each holding a list of measured value lists, "
                "one per date, in the order given by 'measured_cols'."
            ),
        ),
        meta.Port(
            name="monica",
            contentType="Text (JSON)",
            desc=(
                "JSON object keyed by group (e.g. TREAT_ID), each holding a list of [ISO date, MONICA "
                "outputs] pairs, one per date, matching the 'measured' values position by position."
            ),
        ),
    ],
    config=CompConfig,
)


class ObsToCalibData(process.Process[CompConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def measured_value(self, row: dict[str, Any], cols: str | list[str]) -> float | None:
        if isinstance(cols, str):
            return row.get(cols)
        # summed columns: a missing value counts as 0, but a sum of nothing but missing values stays
        # unknown instead of becoming a measured 0
        values = [v for v in (row.get(c) for c in cols) if v is not None]
        return sum(values) if values else None

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        while self.in_ports["in"] and (self.out_ports["measured"] or self.out_ports["monica"]):
            try:
                in_ip = await self.read_in("in")
                if in_ip is None:
                    break

                rows: list[dict[str, Any]] = json.loads(in_ip.content.as_text())[self.config.data_key]

                measured: dict[str, list[list[float | None]]] = defaultdict(list)
                monica: dict[str, list[list[Any]]] = defaultdict(list)
                for row in sorted(rows, key=lambda r: str(r[self.config.date_col])):
                    values = [self.measured_value(row, cols) for cols in self.config.measured_cols]
                    # nothing was measured on that date, so there is nothing to calibrate against
                    if all(v is None for v in values):
                        continue
                    group = str(row[self.config.group_col])
                    measured[group].append(values)
                    monica[group].append([row[self.config.date_col], self.config.monica_outputs])
                logger.info(
                    "%s: %d group(s), %d date(s) with measurements",
                    self.name,
                    len(measured),
                    sum(len(v) for v in measured.values()),
                )

                for port_name, content in (("measured", measured), ("monica", monica)):
                    if not self.out_ports[port_name]:
                        continue
                    out_ip = fbp_capnp.IP.new_message(content=json.dumps(content))
                    common.copy_and_set_fbp_attrs(in_ip, out_ip)
                    if not await self.write_out(port_name, out_ip):
                        logger.info("%s: process finished", self.name)
                        return

            except Exception:
                logger.exception("%s", self.name)

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(ObsToCalibData(METADATA), METADATA)


if __name__ == "__main__":
    main()
