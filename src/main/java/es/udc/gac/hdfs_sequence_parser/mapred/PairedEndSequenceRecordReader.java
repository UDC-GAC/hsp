/*
 * Copyright (C) 2017 Universidade da Coruña
 * 
 * This file is part of ___.
 * 
 * ___ is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ___ is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with HSRA. If not, see <http://www.gnu.org/licenses/>.
 */

package es.udc.gac.hdfs_sequence_parser.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 
 * @author Roberto Rey Exposito		<rreye@udc.es>
 * @author Luis Lorenzo Mosquera	<luis.lorenzom@udc.es> 
 */
public class PairedEndSequenceRecordReader extends RecordReader<LongWritable, Text> {

	private static final Text PAIRED_END_MARKER_TEXT = new Text("|");
	public static final String PAIRED_END_MARKER = PAIRED_END_MARKER_TEXT.toString();

	// Sequence record readers to get key-value pairs from single-end datasets
	private SingleEndSequenceRecordReader leftRR;
	private SingleEndSequenceRecordReader rightRR;
	private LongWritable key;
	private Text value;

	public PairedEndSequenceRecordReader(CompositeInputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		// Create left and right record readers
		try {
			String leftInputFormatClass = context.getConfiguration().get(PairedSequenceInputFormat.LEFT_INPUT_FORMAT, "");
			String rightInputFormatClass = context.getConfiguration().get(PairedSequenceInputFormat.RIGHT_INPUT_FORMAT, "");

			if (!leftInputFormatClass.equals(rightInputFormatClass))
				throw new IOException("Input formats do not match: "+leftInputFormatClass+", "+rightInputFormatClass);

			SequenceTextInputFormat leftInputFormat = (SequenceTextInputFormat) ReflectionUtils.newInstance(Class.forName(leftInputFormatClass), conf);
			SequenceTextInputFormat rightInputFormat = (SequenceTextInputFormat) ReflectionUtils.newInstance(Class.forName(rightInputFormatClass), conf);
			leftRR = (SingleEndSequenceRecordReader) leftInputFormat.createRecordReader(inputSplit.get(0), context);
			rightRR = (SingleEndSequenceRecordReader) rightInputFormat.createRecordReader(inputSplit.get(1), context);
		} catch (ClassNotFoundException e) {
			throw new IOException(e.getMessage());
		}

		key = new LongWritable();
		value = null;
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return Math.min(leftRR.getProgress(), rightRR.getProgress());
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		CompositeInputSplit inputSplit = (CompositeInputSplit) genericSplit;
		leftRR.initialize(inputSplit.get(0), context);
		rightRR.initialize(inputSplit.get(1), context);
		System.out.println("PairedEndSequenceRecordReader initialized");
	}

	@Override
	public synchronized void close() throws IOException {
		leftRR.close();
		rightRR.close();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (leftRR.nextKeyValue()) {
			if(!rightRR.nextKeyValue())
				throw new IOException("Unexpected end of split for right record reader");

			if(leftRR.getCurrentKey().get() != rightRR.getCurrentKey().get())
				throw new IOException("Unexpected different keys");

			value = leftRR.getCurrentValue();

			if(value.getLength() != rightRR.getCurrentValue().getLength())
				throw new IOException("Unexpected different lengths");

			key.set(value.getLength());
			value.append(rightRR.getCurrentValue().getBytes(), 0, rightRR.getCurrentValue().getLength());
			return true;
		}

		if(rightRR.nextKeyValue())
			throw new IOException("Unexpected end of split for left record reader");

		return false;
	}
}