package es.udc.gac.hadoop.sequence.parser.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import es.udc.gac.hadoop.sequence.parser.util.LineReader;

/** The FASTQ reader of HSP before it read the records straight into the value: the reference for FastQReaderEquivalenceTest. */
class LegacyFastQRecordReader extends SingleEndSequenceRecordReader {

	private static final int NUMBER_OF_LINES_PER_READ = 4;
	private static final Text FASTQ_COMMENT_LINE = new Text("+" + LineReader.LF);

	private Text newLine;
	private Text temp;

	public LegacyFastQRecordReader(TaskAttemptContext context) {
		super(context);
		newLine = new Text();
		temp = new Text();
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		int i = 0;
		long firstRead = 0, secondRead = 0;
		boolean found = false;
		value.clear();

		if (isSplitFinished())
			return false;

		while (true) {
			firstRead = readLine(newLine);
			i++;

			if (firstRead == 0) { //EOF
				if (found)
					warnTruncatedRecord();
				return false;
			}

			if (found && i == NUMBER_OF_LINES_PER_READ) {
				value.append(newLine.getBytes(), 0, newLine.getLength());
				break;
			}

			if (newLine.getBytes()[0] == '@') {
				boolean firstHeader = !found;
				found = true;

				secondRead = readLine(temp);

				if (secondRead == 0) { //EOF
					if (!firstHeader)
						warnTruncatedRecord();
					return false;
				}

				if (temp.getBytes()[0] != '@') {
					if (firstHeader && !startRecord(pos - firstRead - secondRead))
						return false;

					i = 2;
					if (getTrimSequenceName()) {
						//Trim spaces in sequence name
						LineReader.trim(newLine, 2);
					}
					newLine.append(temp.getBytes(), 0, temp.getLength());
				} else {
					if (firstHeader && !startRecord(pos - secondRead))
						return false;

					i = 1;
					if (getTrimSequenceName()) {
						//Trim spaces in sequence name
						LineReader.trim(temp, 2);
					}
					value.append(temp.getBytes(), 0, temp.getLength());
					continue;
				}
			}

			if (found) {
				if (i != 3)
					value.append(newLine.getBytes(), 0, newLine.getLength());
				else
					value.append(FASTQ_COMMENT_LINE.getBytes(), 0, FASTQ_COMMENT_LINE.getLength());
				continue;
			}

			if (i == NUMBER_OF_LINES_PER_READ) {
				i = 0;
			}
		}

		return true;
	}
}
