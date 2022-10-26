#!/usr/bin/python3.6

from os import system
from flask import Flask, request, jsonify
from search import search, createIndex

import whoosh
from whoosh.fields import TEXT, Schema
from whoosh.index import open_dir

from datetime import datetime

import re

from flask_cors import CORS

#base flask app
app = Flask(__name__)
CORS(app)

@app.route("/", methods=["GET"])
def homePage():
	return "Flask server loaded. Happy fishing!"

@app.route("/params=<plist>", methods=["GET"])
def fetchResults(plist):
	print("plist=", plist)
	#search the whoosh index for the query
	fields = ["id", "taxonomy", "commonName", "image", "remark", "tempMin", "tempMax", "phMin", "phMax", "bio1", "bio2", "distribution", "url"]

	if request.method == "GET":
		plist = plist.split("$")
		query = {}
		for i in range(0, len(fields)):
			try:
				if plist[i] != "none":
					query[fields[i]] = re.sub("\+", " ", plist[i])
					query[fields[i]] = re.sub("\_", " ", plist[i])
				else:
					query[fields[i]] = None
			except IndexError:
				print("out of bounds")
				break


		print("search query=", query)
		#search for the query disjunctively

		searchResults = search(query, True, limit=None)

		message = {
			"status": 200,
			"message": "OK",
			"results": searchResults
		}
		return jsonify(message)

def main():
	#create/open whoosh index and schema
	createIndex("fishbase-data2.csv")

	#start the server
	app.run(debug=True)


if __name__ == '__main__':
	pass
else:
	print("not running as main")
	createIndex("fishbase-data2.csv")
	app.config.update(
		SERVER_NAME='localhost:5000',
		APPLICATION_ROOT='/',
	)
