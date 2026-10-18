/*
 * Copyright (C) 2020 Universidade da Coruña
 * 
 * This file is part of HSP.
 * 
 * HSP is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * HSP is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with HSP. If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.hadoop.sequence.parser.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import es.udc.gac.hadoop.sequence.parser.util.LineReader;

/**
 * 
 * @author Roberto Rey Exposito		<rreye@udc.es>
 * @author Luis Lorenzo Mosquera	<luis.lorenzom@udc.es>
 * @author Jorge González-Domínguez	<jgonzalezd@udc.es> 
 */
public class FastARecordReader extends SingleEndSequenceRecordReader {

	private Text newLine;
	/* The header of the next record is read while finding the end of the current one, so it is kept in newLine for the next call */
	private boolean pendingHeader;
	private int pendingHeaderLength;

	public FastARecordReader(TaskAttemptContext context) {
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
			} else if (found && peek() != '>') {
				//A line of the sequence of the current record: straight into value
				read = appendLine(value);
				if (read == 0) //EOF
					break;
				continue;
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
