/*
 * Copyright (C) 2017 Universidade da Coruña
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Text-based InputFormat for single-end sequence files.
 * 
 * @author Roberto Rey Exposito <rreye@udc.es>
 * @author Luis Lorenzo Mosquera <luis.lorenzom@udc.es>
 */
public abstract class SingleEndSequenceInputFormat extends FileInputFormat<LongWritable, Text> {

	protected static final double SPLIT_SLOP = 1.1; // 10% slop

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		if (null == codec) {
			return true;
		}
		return codec instanceof SplittableCompressionCodec;
	}

	/**
	 * Get the number of possible splits from one file 
	 * 
	 * @param inputPath
	 * @param File's length
	 * @param if the file can be spliteble
	 * @param the split's size 
	 * @return The number of file's splits
	 * @throws IOException if you have any problem opening or reading the file
	 */
	public static int getNumberOfSplits(Path inputPath, long inputPathLength, boolean inputPathSplitable,
			long splitSize) throws IOException {

		int nsplits = 0;
		long length = inputPathLength;

		if (length != 0) {
			if (inputPathSplitable) {
				long bytesRemaining = length;

				while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
					nsplits++;
					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0)
					nsplits++;
			} else { // not splitable
				nsplits = 1;
			}
		}

		return nsplits;
	}
}
