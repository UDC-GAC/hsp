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
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * RecordReader which breaks the data of paired-end sequence files in key/value pairs (LongWritable/Text)
 * 
 * @author Roberto Rey Exposito		<rreye@udc.es>
 * @author Luis Lorenzo Mosquera	<luis.lorenzom@udc.es>
 * @author Jorge González-Domínguez	<jgonzalezd@udc.es>
 */
public class PairedEndSequenceRecordReader extends RecordReader<LongWritable, PairText> {

	// Sequence record readers to get key-value pairs from single-end datasets
	private SingleEndSequenceRecordReader leftRR;
	private SingleEndSequenceRecordReader rightRR;
	private LongWritable key;
	private PairText value;
	private Text left, right;

	public PairedEndSequenceRecordReader(PairedEndInputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		// Create left and right record readers
		try {
			String leftInputFormatClass = context.getConfiguration().get(PairedEndSequenceInputFormat.LEFT_INPUT_FORMAT, "");
			String rightInputFormatClass = context.getConfiguration().get(PairedEndSequenceInputFormat.RIGHT_INPUT_FORMAT, "");

			if (!leftInputFormatClass.equals(rightInputFormatClass))
				throw new IOException("Input formats do not match: "+leftInputFormatClass+", "+rightInputFormatClass);

			SingleEndSequenceInputFormat leftInputFormat = (SingleEndSequenceInputFormat) ReflectionUtils.newInstance(Class.forName(leftInputFormatClass), conf);
			SingleEndSequenceInputFormat rightInputFormat = (SingleEndSequenceInputFormat) ReflectionUtils.newInstance(Class.forName(rightInputFormatClass), conf);
			leftRR = (SingleEndSequenceRecordReader) leftInputFormat.createRecordReader(inputSplit.get(0), context);
			rightRR = (SingleEndSequenceRecordReader) rightInputFormat.createRecordReader(inputSplit.get(1), context);
		} catch (ClassNotFoundException e) {
			throw new IOException(e.getMessage());
		}

		key = new LongWritable();
		value = new PairText();
		left = null;
		right = null;
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public PairText getCurrentValue() {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return Math.min(leftRR.getProgress(), rightRR.getProgress());
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		PairedEndInputSplit inputSplit = (PairedEndInputSplit) genericSplit;
		leftRR.initialize(inputSplit.get(0), context);
		rightRR.initialize(inputSplit.get(1), context);
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

			left = leftRR.getCurrentValue();
			right = rightRR.getCurrentValue();

			if(left.getLength() != right.getLength())
				throw new IOException("Unexpected different lengths");

			key.set(leftRR.getCurrentKey().get());
			value.setLeft(left);
			value.setRight(right);
			return true;
		}

		if(rightRR.nextKeyValue())
			throw new IOException("Unexpected end of split for left record reader");

		return false;
	}

	public static String getLeftRead(PairText pairedRead) throws CharacterCodingException {
		return Text.decode(pairedRead.getLeft().getBytes(), 0, pairedRead.getLeft().getLength(), false);
	}

	public static String getRightRead(PairText pairedRead) throws CharacterCodingException {
		return Text.decode(pairedRead.getRight().getBytes(), 0, pairedRead.getRight().getLength(), false);
	}
}