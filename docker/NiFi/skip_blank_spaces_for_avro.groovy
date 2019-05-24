import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

//get the nifi flowfile
flowFile = session.get()

//return if the flow file is null
if (!flowFile) return

// Cast a closure with an inputStream and outputStream parameter to StreamCallback
flowFile = session.write(flowFile, { inputStream, outputStream ->

    //load the flowfile content
    text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)

    //boolean variable to handle header special case in filtering
    def isHeader = true

    //split the entire csv file in rows
    def rows = text.split("\n")

    //iterate over each row
    for (row in rows) {

        //new row
        def newText = ''

        //if we're reading the header
        if (isHeader) {

            //replace spaces with underscores to be Avro Format friendly
            newText += row.replaceAll(" ", "_")

            //add new line
            newText += "\n"

            //there is only one header in file
            isHeader = false

            //write the csv header
            outputStream.write(newText.getBytes(StandardCharsets.UTF_8))
            continue
        }

        //write each row as they are
        newText += row
        newText += "\n"
        outputStream.write(newText.getBytes(StandardCharsets.UTF_8))
    }

} as StreamCallback)

//transfer file to the success relation
session.transfer(flowFile, REL_SUCCESS)