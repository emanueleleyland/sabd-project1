@Grab('com.xlson.groovycsv:groovycsv:1.3')
import static com.xlson.groovycsv.CsvParser.parseCsv
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat

//get the nifi flowfile
flowFile = session.get()

//return if the flow file is null
if (!flowFile) return

// Cast a closure with an inputStream and outputStream parameter to StreamCallback
flowFile = session.write(flowFile, { inputStream, outputStream ->
    //read file as string
    text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)

    // Read line as header
    def isHeader = true

    //Date parser
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //Parse csv file using first line as header
    def rows = parseCsv(text, readFirstLine: false)

    //fill a vector with columns name, this is used to access every row field as map
    def columns = rows.columns.keySet() as String[]
    String[] app_list = new String[columns.length]


    for (row in rows) {
        //newline
        def newText = ''

        //if we're reading the header
        if (isHeader) {
            newText += columns.join(',')
            newText += "\n"
            isHeader = false
        }

        try {
            //Parse date column. If value is not valid row won't be written
            sdf.parse((String) row.getAt('datetime'))

            for (int value = 0; value < columns.length; value++) {
                //first column is a date
                if (value == 0) {
                    app_list[0] = row.getAt('datetime')
                    continue
                }
                //Pass empty values
                if(row.getAt(columns[value]) == '') {
                    app_list[value] = ''
                    continue
                }
                //check whether humidity value is valid (between 0% and 100%). Set empty value if not valid
                if(Double.valueOf(row.getAt(columns[value])) < 0.0 || Double.valueOf(row.getAt(columns[value])) > 100.0) {
                    app_list[value] = ''
                }
                else {
                    //copy valid measure
                    app_list[value] = row.getAt(columns[value])
                }
            }

            //build new row
            for (int j = 0; j < app_list.length; j++){
                if (j == app_list.length - 1) newText += app_list[j]
                else newText += app_list[j] + ","
            }
            newText += "\n"
            outputStream.write(newText.getBytes(StandardCharsets.UTF_8))
        } catch (Exception e) {
            continue
        }

    }

} as StreamCallback)
//transfer file to success relation
session.transfer(flowFile, REL_SUCCESS)