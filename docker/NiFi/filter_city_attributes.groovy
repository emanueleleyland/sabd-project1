import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
//get the nifi flowfile
flowFile = session.get()

//return if the flow file is null
if (!flowFile) return

// Cast a closure with an inputStream and outputStream parameter to StreamCallback
flowFile = session.write(flowFile, { inputStream, outputStream ->
    text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)

    // Read line as header
    def isHeader = true

    // Latitude and longitude column index for csv file
    def LATITUDE = 1
    def LONGITUDE = 2

    // base URL service to get timezone ID from latitude and longitude
    def url = 'http://api.geonames.org/timezoneJSON?'

    //split the entire csv file in rows
    def rows = text.split('\n')

    for (row in rows) {
        //new filtered row
        def newText = ''

        //if we're reading the header
        if (isHeader) {
            //add new two column to header
            newText += row + ",Country,TimeUTC"
            newText += "\n"

            //no more read header
            isHeader = false

            //write the csv header
            outputStream.write(newText.getBytes(StandardCharsets.UTF_8))
            continue
        }

        //split each csv value in a vector
        def values = row.split(",")

        //check whether latitude and longitude values are valid for each city
        if (Float.valueOf(values[LATITUDE]) >= -90 && Float.valueOf(values[LATITUDE]) <= 90 && Float.valueOf(values[LONGITUDE]) >= -180 && Float.valueOf(values[LONGITUDE]) <= 180) {
            //GET request to api.geonames.org to obtain Country and timezone ID from coordinates
            String tempurl = url + 'lat='+ values[LATITUDE] +'&lng=' + values[LONGITUDE] + '&username=sabdpr1'
            String result = new URL(tempurl).text

            //deserialize Json response
            JsonSlurper slurper = new JsonSlurper()
            def r = slurper.parseText(result)
            country = r.getAt('countryName')
            timezone = r.getAt('timezoneId')

            //build and write newline
            newText += values.join(',') + "," + country + "," + timezone
            newText += "\n"
            outputStream.write(newText.getBytes(StandardCharsets.UTF_8))
        }

    }

} as StreamCallback)

//transfer file to success relation
session.transfer(flowFile, REL_SUCCESS)