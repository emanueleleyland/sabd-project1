@Grab('com.xlson.groovycsv:groovycsv:1.3')
import static com.xlson.groovycsv.CsvParser.parseCsv
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
//get the nifi flowfile
flowFile = session.get()

//return if the flow file is null
if (!flowFile) return
// Cast a closure with an inputStream and outputStream parameter to StreamCallback
flowFile = session.write(flowFile, { inputStream, outputStream ->
	//read inputStream as buffered reader
	BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))

	// Read line as header
	def isHeader = true

	//datetime parser
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


	String csvRow
	Iterator iter

	//header columns name
	def columns
	String[] app_list

	//a line is left to read
	while (reader.ready()) {


		csvRow = reader.readLine()
		//new filtered row
		def newText = ''

		//if we're reading the header
		if (isHeader) {
			columns = csvRow.split(",")
			app_list = new String[columns.length]
			csvRow += "\n"

			//no more read header
			isHeader = false

			//write the csv header
			outputStream.write(csvRow.getBytes(StandardCharsets.UTF_8))
			continue
		}

		//parse line as csv line
		iter = parseCsv([readFirstLine: true, columnNames: columns], csvRow)


		for (row in iter) {
			try {
				//parse datetime. If is not correct skip the row
				sdf.parse((String) row.getAt('datetime'))

				//loop over values is csv line
				for (int value = 0; value < columns.length; value++) {
					//write datetime to filtered row
					if (value == 0) {
						app_list[0] = row.getAt('datetime')
						continue
					}
					//write empty values
					if (row.getAt(columns[value]) == '') {
						app_list[value] = ''
						continue
					}
					//write empty value if it is out of range (between 870.0hPa and 1085.6hPa)
					if (Double.valueOf(row.getAt(columns[value])) < 870.0 || Double.valueOf(row.getAt(columns[value])) > 1085.6) {
						app_list[value] = ''

					} else {
						app_list[value] = row.getAt(columns[value])

					}
				}
				//aggregate values in a new csv row and write it to outputStream
				for (int j = 0; j < app_list.length; j++) {
					if (j == app_list.length - 1) newText += app_list[j]
					else newText += app_list[j] + ","
				}
				newText += "\n"
				outputStream.write(newText.getBytes(StandardCharsets.UTF_8))

			} catch (Exception e) {
				continue
			}
		}
	}

} as StreamCallback)
//transfer file to success relation
session.transfer(flowFile, REL_SUCCESS)