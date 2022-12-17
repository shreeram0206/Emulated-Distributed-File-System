import sys
import os
from flask import Flask, request, render_template
from flask import send_file
from flask import Response

sys.path.insert(0, r'C:\Users\User\hello\DSCI551\Emulated-Distributed-File-System\src\mongodb')  # To handle import of module metadata (temporary)

from metadata import MongoMetadata
from searchanalytics import SearchAnalytics

app = Flask(__name__)

ACCEPTED_COMMANDS = ['mkdir', 'ls', 'rm', 'put', 'cat', 'rmdir']
metadata = MongoMetadata()
searchanal = SearchAnalytics()

COMMAND_DICT = {"mkdir" : metadata.mkdir,
                "ls" : metadata.ls,
                "rm" : metadata.rm,
                "cat" : metadata.cat,
                "put" : metadata.put}

@app.route("/", methods=["GET"])
def landing_page():
    return render_template("landing_page.html")


@app.route("/search", methods=["GET", "POST"])
def search():
    if request.method == "POST":
        identifier = request.json.get("identifier")
        path = request.json.get("command")
        stock_name = request.json.get("stock_name")
        method_name = request.json.get("method_name")
        attribute_name = request.json.get("attribute_name")
        if identifier == "search":
            search_data = searchanal.searchDataset(path, [stock_name, method_name, attribute_name])
            return {'response': search_data}
        elif identifier == "dropdown":
            dropdown_options = searchanal.dropdown(path)
            dropdown_menu = [dropdown_options[0], dropdown_options[1], dropdown_options[2]]  ## Sample output (list of lists is also fine)
            return {'response': dropdown_menu}

    return render_template("search.html")


@app.route("/analytics", methods=["GET", "POST"])
def analytics():
    if request.method == "POST":
        dataset_name = request.json.get("command")
        stock_name = request.json.get("attribute_option")
        identifier = request.json.get("identifier")
        if identifier == "analyze":
            if dataset_name == "Population Data":
                dataset = "population.csv"
            elif dataset_name == "Housing Data":
                dataset = "house.csv"
            elif dataset_name == "Stocks Data":
                dataset = "stocks.csv"
            data = searchanal.analyseDataset(dataset, identifier, stock_name)
            return {'response': data}
        elif identifier == "cat":
            data = searchanal.analyseDataset(dataset_name, identifier)
            return {'response': data}

    return render_template("analytics.html")

@app.route('/terminal', methods =["GET", "POST"])
def enter_commands():
    if request.method == "POST":
        command = request.json.get("command")
        commands_split = command.split(" ")
        if commands_split[0] == "put":
            data = COMMAND_DICT.get(commands_split[0])(commands_split[1], commands_split[2], int(commands_split[3]))
        else:
            data = COMMAND_DICT.get(commands_split[0])(commands_split[1])
        if data is None:
            data = ["Not supported command"]
        elif type(data) != list:
            data = ["Something went wrong"]
        return {'response': data}

    return render_template("terminal.html")

@app.route('/interface', methods =["GET", "POST"])
def userInterface():
    print('CHECKPOINT', request, request.method)
    if request.method == "POST":
        command = request.json.get("command")
        identifier = request.json.get("identifier")
        if identifier == "ls":
            if command == "root":
                cmd = "/"
                folders = metadata.ls(cmd)
            else:
                folders = metadata.ls(command)
            return {"response" : folders}
        elif identifier == "mkdir":
            metadata.mkdir(command)
        elif identifier == "rm":
            metadata.rm(command)
        elif identifier == "cat":
            cat_resp = metadata.cat(command)
            return {"response" : cat_resp}

    return render_template("index.html")

@app.route('/images/<path>')
def get_dir(path):
    return send_file("images/"+path)
 
if __name__=='__main__':
   app.run(debug=True) # host:localhost, port:5000
