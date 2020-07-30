#Jacob Pugsley
#CS 483 - Web Data
#November 5, 2019

import sys
import argparse
import re
from decimal import Decimal
from whoosh import index
from whoosh.index import open_dir
from whoosh import qparser
from whoosh.qparser import MultifieldParser, QueryParser
from whoosh.fields import TEXT, ID, NUMERIC, Schema


def main():
	#command line arguments
	parser = argparse.ArgumentParser()
	parser.add_argument("filename", help="the csv file to search through")
	parser.add_argument("query", help="what to search for")
	parser.add_argument("-c", "--conjunctive", help="search for ALL of the query terms", action="store_true")
	parser.add_argument("-d", "--disjunctive", help="search for ANY of the query terms", action="store_true")
	parser.add_argument("-l", "--limit", help="maximum number of tuples returned", type=int, default=10)
	parser.add_argument("-j", "--json", help="print")
	args = parser.parse_args()

	#error if the file is not a CSV
	if not ".csv" in args.filename:
		print("The file must be in CSV format.")
		return

	#determine whether to search conjunctively or disjunctively
	#  based on the command line options
	dis = False
	if args.conjunctive:
		print("Using conjunctive search")
	elif args.disjunctive:
		print("Using disjunctive search")
		dis = True
	else:
		print("Using conjunctive search")

	#build the index and search for the user query
	createIndex(args.filename)
	search(args.query, disjunctive=dis, limit=args.limit)







def createIndex(file):
	print("Loading index")
	#create a schema
	global schema
	schema = Schema(
		id=ID(stored=True),
		taxonomy=TEXT(stored=True),
		commonName=TEXT(stored=True),
		image=TEXT(stored=True),
		remark=TEXT(stored=True),
		tempMin=TEXT(stored=True), #default to ints
		tempMax=TEXT(stored=True),
		phmin=TEXT(stored=True),
		phmax=TEXT(stored=True),
		bio1=TEXT(stored=True),
		bio2=TEXT(stored=True),
		distribution=TEXT(stored=True),
		url=TEXT(stored=True)
	)


	global ind
	if not index.exists_in("fishindex"):
		print("No index found, creating new index")
		#create index
		ind = index.create_in("fishindex", schema)

		print("Parsing CSV")

		wtr = ind.writer()

		previousItem = []

		for item in csvRdr(file):
			#if the item has the same id as the last item,
			#  update the information
			if len(item) < 19:
				continue

			if previousItem == []:
				previousItem = item

			elif item[0] == previousItem[0]:
				#update
				for i in range(0, len(previousItem)):
					#fill in the blank fields
					if previousItem[i] == '':
						previousItem[i] = item[i]
			else:
				#add the previous item as a document
				wtr.add_document(
					id=previousItem[0],
					taxonomy=previousItem[1],
					commonName=previousItem[2],
					image=previousItem[3],
					remark=previousItem[4],
					tempMin=previousItem[5],
					tempMax=previousItem[6],
					phmin=previousItem[7],
					phmax=previousItem[8],
					bio1=previousItem[9],

					bio2=previousItem[10],
					
					#skip 11 as it is just the same taxonomy
					#distribution is a coordinate min/max that can be
					#  interpreted by Google Maps
					distribution=previousItem[12] + " " + previousItem[13] + " " + previousItem[14] + " " + previousItem[15] + " " + previousItem[16] + " " + previousItem[17],
					url=previousItem[18]
				)

				#update the previous item
				previousItem = item

		#this closes the writer
		wtr.commit()
	else:
		#open index
		print("Using existing index")
		ind = open_dir("fishindex")

	
def csvRdr(file="sample.csv"):
	#yield each row of a given CSV file for indexing
	with open(file) as csv:
		firstLine = True
		previousRow = []
		for row in csv:
			#skip the header
			if firstLine:
				firstLine = False
				continue

			#determine if this is a new row or just a newline
			appendToLast = False
			try:
				int(row[0])
			except(ValueError):
				appendToLast = True
			

			#split on commas
			parsedRow = []
			currentString = ""
			inQuote = False
			for char in row:
				#if a comma is found outside of a quote,
				#  add the current string to the parsed row
				if char == ',' and not inQuote:
					#print("appending ", currentString)
					parsedRow.append(currentString)
					currentString = ""

				#enter or exit quote when a " is found
				elif char == '"' and inQuote:
					#print("end quote")
					inQuote = False
				elif char == '"' and not inQuote:
					#print("begin quote")
					inQuote = True

				#add all other chars to the current
				#  string
				else:
					currentString = currentString + char
			
			#append the last string

			parsedRow.append(currentString)


			#hand back all the parsed rows
			yield parsedRow

def search(query, disjunctive=False, limit=10):
	#search all fields in the schema for the query terms
	print("Searching for", query, end="\n\n")
	#https://whoosh.readthedocs.io/en/latest/querylang.html
	#parse the query terms into fields

	content = ""
	for field in query:
		if query[field] is not None and query[field] != '': 
			if field == "phMin":
				content = content + "(phmin:[" + query["phMin"] + " TO " + query["phMax"] + "] "
				content = content + "OR phmax:[" + query["phMin"] + " TO " + query["phMax"] + "]) AND "
			elif field == "phMax":
				continue
			elif field == "tempMin":
				content = content + "(tempMin:[" + query["tempMin"] +  " TO " + query["tempMax"] + "] "
				content = content + "OR tempMax:[" + query["tempMin"] +  " TO " + query["tempMax"] + "]) AND "
			elif field == "tempMax":
				continue
			else:
				content = content + field + ":'"+query[field] + "' AND "
	content = content[:len(content)-5]

	print("content=", content)

	parser = QueryParser("id", schema)

	print("got query parser")

	p = parser.parse(content)

	print("parsed content")

	#search for the query terms
	with ind.searcher() as searcher:
		results = searcher.search(p, limit=limit)
		
		print(results)

		for r in results:
			print(r)


		resultDict = {} #contains all results
		i = 0
		for r in results:
			resultDict[str(i)] = {}
			resultDict[str(i)]["id"] = r["id"]
			resultDict[str(i)]["taxonomy"] = r["taxonomy"]
			resultDict[str(i)]["commonName"] = r["commonName"]
			resultDict[str(i)]["image"] = r["image"]
			resultDict[str(i)]["remark"] = r["remark"]
			resultDict[str(i)]["tempMin"] = r["tempMin"]
			resultDict[str(i)]["tempMax"] = r["tempMax"]
			resultDict[str(i)]["phMin"] = r["phmin"]
			resultDict[str(i)]["phMax"] = r["phmax"].strip('\n')
			resultDict[str(i)]["bio1"] = r["bio1"]
			resultDict[str(i)]["bio2"] = r["bio2"]
			resultDict[str(i)]["distribution"] = r["distribution"]
			resultDict[str(i)]["url"] = r["url"]
			i = i + 1
		return resultDict

if __name__ == '__main__':
	main()
