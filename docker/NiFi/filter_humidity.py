import os
import sys
import csv
import datetime
import java.io
from StringIO import StringIO
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

DEFAULT_DELIMITER = ','
DATE = 0


class ModJSON(StreamCallback):

	def __init__(self):
		pass

	# City attribute
	def process(self, inputStream, outputStream):

		text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
		# Read the CSV stream.

		delim = ','
		renum = False
		isHeader = True
		file_like_io = StringIO(text)
		csv_reader = csv.reader(file_like_io, dialect='excel', delimiter=delim)

		for row in csv_reader:
			newText = ''
			if isHeader:
				newText += ",".join(row)
				newText += "\n\r"
				isHeader = False
				outputStream.write(newText)
				continue

			try:
				datetime.datetime.strptime(row[DATE], '%Y-%m-%d %H:%M:%S')
			except ValueError:
				continue

			for value in range(1, len(row)):
				if row[value] == '':
					continue
				if float(row[value]) < 0.0 or float(row[value]) > 100.0:
					# out of range
					row[value] = ''
			newText += ",".join(row)
			newText += "\n\r"

			outputStream.write(newText)


flowFile = session.get()
if (flowFile != None):
	flowFile = session.write(flowFile, ModJSON())
	session.transfer(flowFile, REL_SUCCESS)
	session.commit()
