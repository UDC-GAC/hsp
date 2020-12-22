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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.udc.gac.hadoop.sequence.parser.util.LineReader;

/**
 * 
 * @author Roberto Rey Exposito		<rreye@udc.es>
 * @author Luis Lorenzo Mosquera	<luis.lorenzom@udc.es>
 * @author Jorge González-Domínguez	<jgonzalezd@udc.es> 
 */
public class FastARecordReader extends SingleEndSequenceRecordReader {

	private static final Logger logger = LogManager.getLogger();
	private Text newLine;
	private long numReads;

	public FastARecordReader(TaskAttemptContext context) {
		super(context);
		newLine = new Text();
		numReads = 0;
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		long read = 0;
		boolean found = false;
		value.clear();

		logger.trace("start {}, end {}, splitPos {}", start, end, getSplitPosition());

		if (isSplitFinished())
			return false;

		while (true) {
			read = readLine(newLine);

			logger.trace("start {}, end {}, splitPos {}", start, end, getSplitPosition());

			if (read == 0) {
				// EOF
				if (found)
					break;
				return false;
			}

			if (newLine.getBytes()[0] == '>') {
				logger.trace("starting '>' has been found");

				if (found) {
					seek(getLineReaderPosition() - read);
					pos -= read;
					break;
				} else {
					numReads++;
					key.set(start+numReads);
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

		logger.trace("start {}, end {}, splitPos {}", start, end, getSplitPosition());

		return true;
	}
}
