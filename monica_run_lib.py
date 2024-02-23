#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Susanne Schulz <susanne.schulz@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import csv
import gzip
import json
import numpy as np
from datetime import date, timedelta


def read_csv(path_to_setups_csv, key="run-id", key_type=(int,), header_row_line=1, data_row_start=2):
    """read sim setup from csv file"""
    composite_key = type(key) is tuple
    keys = {i: v for i, v in enumerate(key)} if composite_key else {0: key}
    key_types = {i: v for i, v in enumerate(key_type)}

    with open(path_to_setups_csv) as _:
        key_to_data = {}
        # determine seperator char
        dialect = csv.Sniffer().sniff(_.read(), delimiters=';,\t')
        _.seek(0)
        # read csv with seperator char
        reader = csv.reader(_, dialect)
        line = 1
        while line < header_row_line:
            next(reader)
            line += 1
        header_cols = next(reader)
        line += 1
        while line < data_row_start:
            next(reader)
            line += 1

        for row in reader:
            data = {}
            for i, header_col in enumerate(header_cols):
                value = row[i]
                if value.lower() in ["true", "false"]:
                    value = value.lower() == "true"
                if composite_key and header_col in keys.values():
                    for i, k in keys.items():
                        if header_col == k:
                            value = key_types.get(i, key_types[0])(value)
                            break
                elif header_col == key:
                    value = key_types[0](value)
                data[header_col] = value
            if composite_key:
                key_vals = tuple([key_types.get(i, key_types[0])(data[k]) for i, k in keys.items()])
            else:
                key_vals = key_types[0](data[key])
            key_to_data[key_vals] = data
        return key_to_data


def read_sim_setups(path_to_setups_csv):
    "read sim setup from csv file"
    with open(path_to_setups_csv) as setup_file:
        setups = {}
        # determine seperator char
        dialect = csv.Sniffer().sniff(setup_file.read(), delimiters=';,\t')
        setup_file.seek(0)
        # read csv with seperator char
        reader = csv.reader(setup_file, dialect)
        header_cols = next(reader)
        for row in reader:
            data = {}
            if len(row) == 0:
                continue
            for i, header_col in enumerate(header_cols):
                value = row[i]
                if value.lower() in ["true", "false"]:
                    value = value.lower() == "true"
                if i == 0:
                    value = int(value)
                data[header_col] = value
            setups[int(data["run-id"])] = data
        return setups


def read_header(path_to_ascii_grid_file, no_of_header_lines=6):
    """read metadata from esri ascii grid file"""

    def read_header_from(f):
        possible_headers = ["ncols", "nrows", "xllcorner", "yllcorner", "cellsize", "nodata_value"]
        metadata = {}
        header_str = ""
        for i in range(0, no_of_header_lines):
            line = f.readline()
            s_line = [x for x in line.split() if len(x) > 0]
            key = s_line[0].strip().lower()
            if len(s_line) > 1 and key in possible_headers:
                metadata[key] = float(s_line[1].strip())
                header_str += line
        return metadata, header_str

    if path_to_ascii_grid_file[-3:] == ".gz":
        with gzip.open(path_to_ascii_grid_file, mode="rt") as _:
            return read_header_from(_)

    with open(path_to_ascii_grid_file, mode="rt") as _:
        return read_header_from(_)


def get_value(list_or_value):
   return list_or_value[0] if isinstance(list_or_value, list) else list_or_value
