import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat

//get the nifi flowfile
flowFile = session.get()

//return if the flow file is null
if (!flowFile) return

// Cast a closure with an inputStream and outputStream parameter to StreamCallback
flowFile = session.write(flowFile, { inputStream, outputStream ->
    //save all file as string
    text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)

    //boolean variable to handle header special case in filtering
    def isHeader = true

    //Datetime parser
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //split entire csv file in rows
    def rows = text.split('\n')

    for (row in rows) {
        //filtered row
        def newText = ''

        //process and write header once
        if (isHeader) {
            newText += row
            newText += "\n"
            isHeader = false
            outputStream.write(newText.getBytes(StandardCharsets.UTF_8))
            continue
        }

        //split csv row in a vector
        def values = row.split(",")

        //Try to parse 'datetime' column. If date is malformed row is not written
        //to outputstream
        try {
            sdf.parse((String) values[0])
            newText += row
            newText += "\n"
            outputStream.write(newText.getBytes(StandardCharsets.UTF_8))
        } catch (Exception e) {
            continue
        }

    }

} as StreamCallback)
//transfer file to the success relation
session.transfer(flowFile, REL_SUCCESS)