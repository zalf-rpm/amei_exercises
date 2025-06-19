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

import json
import sys
import zmq
from zalfmas_common import common

def run_producer():
    context = zmq.Context()
    socket = context.socket(zmq.PUSH) # pylint: disable=no-member
    config = {
        "port": "6666",
        "server": "localhost",
    }
    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)
    socket.connect("tcp://" + config["server"] + ":" + config["port"])
    with open("../maricopa_wheat_face/env_1.json", "r") as _:
        env = json.load(_)
    socket.send_json(env)
    print("done")

if __name__ == "__main__":
    run_producer()