# ------------------------------------------------------------------------------
#  Copyright 2020 Forschungszentrum Jülich GmbH and Aix-Marseille Université
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
# from flask_cors import CORS, cross_origin

app = Flask(__name__)
# CORS(app)

VERSION = 0.1

@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "CoSimServer": VERSION,
    })

@app.route("/write", methods=["POST"])
# @cross_origin()
def write():
    """ Write python script to file. """
    data = request.get_json()

    filename = data.get("filename", "./script.py")
    script = data.get("script", "")

    with open(filename, "w") as f:
        f.write(script)

    return json.dumps(True)

if __name__ == "__main__":
    app.run()