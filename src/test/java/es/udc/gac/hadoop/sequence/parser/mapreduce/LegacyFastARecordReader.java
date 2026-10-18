package es.udc.gac.hadoop.sequence.parser.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import es.udc.gac.hadoop.sequence.parser.util.LineReader;

/** The FASTA reader of HSP before it read the sequence lines straight into the value: the reference for FastAReaderEquivalenceTest. */
class LegacyFastARecordReader extends SingleEndSequenceRecordReader {

	private Text newLine;
	/* The header of the next record is read while finding the end of the current one, so it is kept in newLine for the next call */
	private boolean pendingHeader;
	private int pendingHeaderLength;

	public LegacyFastARecordReader(TaskAttemptContext context) {
		super(context);
		newLine = new Text();
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		long read = 0;
		boolean found = false;
		value.clear();

		if (isSplitFinished())
			return false;

		while (true) {
			if (pendingHeader) {
				read = pendingHeaderLength;
				pos += read;
				pendingHeader = false;
			} else {
				read = readLine(newLine);
			}

			if (read == 0) {
				// EOF
				if (found)
					break;
				return false;
			}

			if (newLine.getBytes()[0] == '>') {
				if (found) {
					pos -= read;
					pendingHeader = true;
					pendingHeaderLength = (int) read;
					break;
				} else {
					if (!startRecord(pos - read))
						return false;

					if (getTrimSequenceName()) {
						//Trim spaces in sequence name
						LineReader.trim(newLine, 1);
					}
					found = true;
				}
			}

			if (found)
				value.append(newLine.getBytes(), 0, newLine.getLength());
		}

		return true;
	}
}
