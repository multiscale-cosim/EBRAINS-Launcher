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

VERSION = 0.1
SCRIPT_DIRPATH="${COSIM_SCRIPT_DIRPATH:-${PWD}}" # TODO Use absolute dir path where files can be stored.

@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "CoSimServer": VERSION,
    })

@app.route("/submit", methods=["POST"])
def submit():
    """ Write script to file and the start launcher. """
    data = request.get_json()

    filename = data.get("filename", os.path.join(SCRIPT_DIRPATH, "script.py"))
    script = data.get("script", "")
    with open(filename, "w") as f:
        f.write(script)

    # Start simulation
    # NOTE sending start simmulation signal via PIPE
    # TODO instead of reading/writing through PIPE, communciate via 0MQ
    print("start simulation!")
    return json.dumps(True)

@app.route("/stop", methods=["GET"])
def stop():

    # Stop simulation

    return json.dumps(True)

if __name__ == "__main__":
    host = sys.argv[1]
    port = sys.argv[2]
    print(f"app server host: {host}, port: {port}")
    app.run(host=host, port=port)