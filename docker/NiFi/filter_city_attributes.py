import os
import sys
import csv
import java.io
from StringIO import StringIO
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback
import requests
import datetime
import time


class ModJSON(StreamCallback):

	def __init__(self):
		pass

	# City attribute
	def process(self, inputStream, outputStream):


		LATITUDE = 1
		LONGITUDE = 2
		url = 'http://api.geonames.org/timezoneJSON?'


		text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
		# Read the CSV stream.

		delim = ','
		renum = False
		isHeader = True
		file_like_io = StringIO(text)
		csv_reader = csv.reader(file_like_io, dialect='excel', delimiter=delim)

		for row in csv_reader:
			newText=''
			if isHeader:
				newText += ",".join(row) + ',Country,TimeUTC'
				newText += "\n\r"
				isHeader = False
				outputStream.write(newText)
				continue

			if -90 <= float(row[LATITUDE]) <= 90 and -180 <= float(row[LONGITUDE]) <= 180:
				tempurl = url + 'lat=%s&lng=%s&username=sabdpr1' % (row[LATITUDE], row[LONGITUDE])
				r = requests.get(tempurl).json()
				country = r.get('countryName')
				timezone = r.get('timezoneId')
				#offset = r.get('rawOffset')

				#newText += ",".join(row) + "," + country + "," + str(offset)
				newText += ",".join(row) + "," + country + "," + timezone
				newText += "\n\r"
				outputStream.write(newText)


flowFile = session.get()
if (flowFile != None):
	flowFile = session.write(flowFile, ModJSON())
	session.transfer(flowFile, REL_SUCCESS)
	session.commit()
