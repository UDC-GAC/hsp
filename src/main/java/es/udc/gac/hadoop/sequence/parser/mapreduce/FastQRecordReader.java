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
public class FastQRecordReader extends SingleEndSequenceRecordReader {

	private static final Logger logger = LogManager.getLogger();
	private static final int NUMBER_OF_LINES_PER_READ = 4;
	private static final Text FASTQ_COMMENT_LINE = new Text("+" + LineReader.LF);

	private Text newLine;
	private Text temp;

	public FastQRecordReader(TaskAttemptContext context) {
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

		logger.trace("init: start {}, end {}, pos {}, splitPos {}", start, end, pos, getSplitPosition());

		if (isSplitFinished())
			return false;

		key.set(pos);

		while (true) {
			firstRead = readLine(newLine);
			i++;

			if (firstRead == 0) //EOF
				return false;

			if (found && i == NUMBER_OF_LINES_PER_READ) {
				logger.trace("last line and starting '@' has been previously found");
				value.append(newLine.getBytes(), 0, newLine.getLength());
				break;
			}

			if (newLine.getBytes()[0] == '@') {
				logger.trace("starting '@' has been found at line {}", i);
				found = true;

				secondRead = readLine(temp);

				if (secondRead == 0) //EOF
					return false;

				if (temp.getBytes()[0] != '@') {
					i = 2;
					if (getTrimSequenceName()) {
						//Trim spaces in sequence name
						LineReader.trim(newLine, 2);
					}
					newLine.append(temp.getBytes(), 0, temp.getLength());
				} else {
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
				logger.trace("last line and no starting '@' has been found (discarding previous data)");
				i = 0;
			}
		}

		logger.trace("finish: start {}, end {}, pos {}, splitPos {}", start, end, pos, getSplitPosition());

		return true;
	}
}
