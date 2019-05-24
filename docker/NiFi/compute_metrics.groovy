import groovy.json.JsonSlurper
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import java.util.LinkedHashMap
import java.math.*

//get the nifi flowfile
flowFile = session.get()

//return if the flow file is null
if (!flowFile) return

// Cast a closure with an inputStream and outputStream parameter to StreamCallback
flowFile = session.write(flowFile, { inputStream, outputStream ->

	//load the flowfile content
	text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)

	//hashmap with <filename, statistics> couples
	LinkedHashMap<String, Object[]> map = [:]

	//parse the json list content file
	JsonSlurper slurper = new JsonSlurper()
	def json = slurper.parseText(text)

	//iterate over the json list
	for (LinkedHashMap obj: json) {

		//get the filename property in json object
		String filename = obj.get("filename")

		//get how much preprocessing the file takes
		double elapsed = (double) obj.get("elapsed")

		//get the statistics of the file
		def agg = map.get(filename)

		//if are no already computed statistics
		if (agg == null) {

			//create new statistics vector
			Object[] val = [elapsed, 0.0, 1]

			//put statistics vector in hashmap
			map.put(filename, val)

		//if there are statistics
		} else {

			//compute Welford average and variance
			agg[2]++
			def delta = elapsed - (double) agg[0]
			agg[0] += delta / (int) agg[2]
			def delta2 = elapsed - (double) agg[0]
			agg[1] += delta * delta2
		}
	}

	//csv file header
	def ret = 'filename,avg,stddev,count\n'

	//write the header on the outcoming flowfile
	outputStream.write(ret.getBytes(StandardCharsets.UTF_8))

	//iterate on each computed statistics
	for (String key: map.keySet()) {

		//new row
		def newLine = ''

		//get the statistics vector
		Object[] val = map.get(key)

		//write filename
		newLine += key + ","

		//write average
		newLine += val[0] + ","

		//write standard deviation
		newLine += Math.sqrt((double)val[1]/(int)val[2]) + ","

		//write number of samples
		newLine += val[2] + "\n"

		//write the csv row as | filename | average | statndard deviation | number of samples |
		outputStream.write(newLine.getBytes(StandardCharsets.UTF_8))
	}

} as StreamCallback)

//transfer file to the success relation
session.transfer(flowFile, REL_SUCCESS)