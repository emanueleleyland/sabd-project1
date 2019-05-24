@Grab(group='org.codehaus.groovy.modules.http-builder',module='http-builder', version='0.7.1')

import org.apache.nifi.controller.status.*
import org.apache.nifi.provenance.*
import groovyx.net.http.RESTClient
import static groovyx.net.http.ContentType.JSON

//url where the NiFi ListenHTTP processor are listening
def url = "http://localhost:9000"

//get all the provenance event in NiFi from the first to the last
def provenanceEvents = context.getEventAccess().getProvenanceEvents(0, (int) context.getEventAccess().getProvenanceRepository().getMaxEventId())

//list of record with elapsed time for all preprocessed file
def list = []

//iterate over the events happened in NiFi
for(ProvenanceEventRecord event: provenanceEvents) {

	//if it is a RECEIVE events means that a file has been read from a source
	if(event.getEventType().equals(ProvenanceEventType.RECEIVE)) {

		//get the flow file uuid
		def uuid = event.getFlowFileUuid()

		//get the timestamp when the file has come in NiFi
		def start = event.getEventTime()

		//iterate over the events happened in NiFi
		for(ProvenanceEventRecord event1: provenanceEvents) {

			//if it is a SEND events means that a file has been written to a sink
			//each preprocessed file has an extra attribute to take in account his first parent UUID before any transformation
			if(event1.getEventType().equals(ProvenanceEventType.SEND) && event1.getAttribute("UUID").equals(uuid)) {

				//get filename attribute
				def filename = event1.getAttribute("filename")

				//compute the end of the preprocessing phase
				def end = event1.getEventTime() + event1.getEventDuration()

				//cast milliseconds to seconds
				def elapsed = (end - start) / 1000

				//json record to collect
				def record = [
					'filename': filename,
					'timestamp': new Date(start).format("yyyy-MM-dd HH:mm:ss"),
					'elapsed' : elapsed
  				]

				//collect elapsed time record
				list.add(record)
			}
		}
	}
}

//create the json object
def jsonObj = new groovy.json.JsonBuilder(list).toPrettyString()

//send the json elapsed time records list over HTTP
def client = new RESTClient(url)
def response = client.post(path: "/stats",
  contentType: JSON,
  body: jsonObj,
  headers: [Accept: 'application/json'])
log.info("Status: " + response.status)