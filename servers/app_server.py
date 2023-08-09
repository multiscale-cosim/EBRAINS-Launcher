# ------------------------------------------------------------------------------
#  Copyright 2020-2023 Forschungszentrum Jülich GmbH and Aix-Marseille Université
# "Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements; and to You under the Apache License,
# Version 2.0. "
#
# Forschungszentrum Jülich
# Institute: Institute for Advanced Simulation (IAS)
# Section: Jülich Supercomputing Centre (JSC)
# Division: High Performance Computing in Neuroscience
# Laboratory: Simulation Laboratory Neuroscience
# Team: Multi-scale Simulation and Design


from flask import Flask, jsonify, json, request
from flask_cors import CORS

import os
import sys

app = Flask(__name__)
CORS(app)
health_registry_manager_proxy = None

VERSION = 0.1
# TODO Use absolute dir path where files can be stored.
SCRIPT_DIRPATH="/home/vagrant/multiscale-cosim/Cosim_NestDesktop_Insite/userland/models/nest_simulator"


@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "CoSimServer": VERSION,
    })


@app.route("/global_state", methods=["GET"])
def global_state():
    # get current global state from orchestrator
    return "In progress..."


@app.route("/submit", methods=["POST"])
def submit():
    """ Write script to file and the start launcher. """
    data = request.get_json()

    filename = data.get("filename", os.path.join(SCRIPT_DIRPATH, "nd_spike_activity_test.py"))
    script = data.get("script", "")
    with open(filename, "w") as f:
        f.write(script)

    # Start simulation
    # NOTE sending start simmulation signal via PIPE
    # TODO instead of reading/writing through PIPE, communciate via 0MQ
    print("start simulation!", flush=True)
    return json.dumps(True)


@app.route("/stop", methods=["GET"])
def stop():

    # Stop simulation

    return json.dumps(True)


if __name__ == "__main__":
    # TODO better arg parsing

    # NOTE The order of parameters is important
    host = sys.argv[1]
    port = sys.argv[2]

    # More parameters (such as network address) can be received to setup a
    # channel with Orchestrator for fetching the state information or to
    # receive the monitoring metrics from resource usage monitors

    # start the app server
    app.run(host=host, port=port)