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
 * along with ____. If not, see <http://www.gnu.org/licenses/>.
 */

package es.udc.gac.hdfs_sequence_parser.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import es.udc.gac.hdfs_sequence_parser.util.Constants;
import es.udc.gac.hdfs_sequence_parser.util.LineReader;

/**
 * RecordReader which breaks the data of any sequence file in pair key/value (LongWritable/Text)
 * 
 * @author Roberto Rey Exposito		<rreye@udc.es>
 * @author Luis Lorenzo Mosquera	<luis.lorenzom@udc.es> 
 */
public abstract class SingleEndSequenceRecordReader extends RecordReader<LongWritable, Text>{

	private FSDataInputStream fileInputStream;
	private CompressionInputStream compressionFileInputStream;
	private Seekable filePos;
	private boolean isCompressedInput;
	private Decompressor decompressor;
	private LineReader lineReader;
	protected LongWritable key;
	protected Text value;
	protected long start;
	protected long end;
	protected long pos;

	public SingleEndSequenceRecordReader() {
		key = new LongWritable();
		value = new Text(new byte[Constants.bufferSize]);
		start = pos = end = 0;
	}
	
	public SingleEndSequenceRecordReader(int bufferSize) {
		key = new LongWritable();
		value = new Text(new byte[bufferSize]);
		start = pos = end = 0;
	}
	
	public abstract boolean nextKeyValue() throws IOException;

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
		if (start == end) {
			return 0.0f;
		}
		else {
			return Math.min(1.0f, (getSplitPosition() - start) / (float)(end - start));
		}
	}
	
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {

		org.apache.hadoop.conf.Configuration conf = context.getConfiguration();
		FileSplit split = (FileSplit) genericSplit;
		Path file = split.getPath();
		start = split.getStart();
		end = start + split.getLength();

		// open the file
		System.out.println("SequenceRecordReader: Open input split "+split.toString());
		fileInputStream = file.getFileSystem(conf).open(file);

		// Check if input file is compressed
		CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);

		if (codec != null) {
			isCompressedInput = true;
			decompressor = CodecPool.getDecompressor(codec);

			if (codec instanceof SplittableCompressionCodec) {
				System.out.println("SequenceRecordReader: Input split is compressed using a splittable codec ("+codec.getClass().getSimpleName()+")");

				// Get split compression input stream
				compressionFileInputStream = ((SplittableCompressionCodec) codec)
						.createInputStream(fileInputStream, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);

				// Create line reader and adjust positions
				lineReader = new LineReader(compressionFileInputStream, conf);
				start = ((SplitCompressionInputStream) compressionFileInputStream).getAdjustedStart();
				end = ((SplitCompressionInputStream) compressionFileInputStream).getAdjustedEnd();
				filePos = compressionFileInputStream;
			} else {
				System.out.println("SequenceRecordReader: Input split is compressed using a non-splittable codec ("+codec.getClass().getSimpleName()+")");

				if (start != 0) {
					/*
					 * We have a split that is only part of a file stored using
					 * a non-splittable codec
					 */
					throw new IOException("Cannot seek in " +
							codec.getClass().getSimpleName() + " compressed stream");
				}

				// Get compression input stream
				compressionFileInputStream = codec.createInputStream(fileInputStream, decompressor);

				// Create line reader and adjust positions
				lineReader = new LineReader(compressionFileInputStream, conf);
				filePos = fileInputStream;
			}

		} else {
			System.out.println("SequenceRecordReader: Input split is not compressed");
			isCompressedInput = false;

			// Seek to the start of the split
			fileInputStream.seek(start);

			// Create line reader and adjust positions
			lineReader = new LineReader(fileInputStream, conf);
			filePos = fileInputStream;
		}

		/**
		 * If this is not the first split, we throw away the first line
		 * because we always (except the last split) read one extra record
		 * in nextKeyValue() method (see also isSplitFinished() method)
		 */
		if (start != 0) {
			start += readLine(value);
			System.out.println("SequenceRecordReader: skipped '"+value+"'");
			value.clear();
		}

		pos = start;

		System.out.println("SequenceRecordReader initialized: start "+start+", end "+end+", splitPos "+getSplitPosition());
	}
	
	@Override
	public synchronized void close() throws IOException {
		try {
			if (lineReader != null) {
				lineReader.close();
			}
		} finally {
			if (decompressor != null) {
				CodecPool.returnDecompressor(decompressor);
				decompressor = null;
			}
		}
	}
	
	
	protected long getSplitPosition() throws IOException {
		if (!isCompressedInput) {
			return pos;
		} else {
			return filePos.getPos();
		}
	}
	
	protected int readLine(Text str) throws IOException {
		int bytesRead = lineReader.readLine(str);
		pos += bytesRead;
		return bytesRead;
	}
	
	protected boolean isSplitFinished() throws IOException {
		/*
		 *  We always read one extra record, which lies outside the 
		 *  upper split limit
		 */
		if (getSplitPosition() > end)
			return true;

		return false;
	}
	
	protected long getLineReaderPosition() throws IOException {
		return lineReader.getPos();
	}
	
	protected void seek(long pos) throws IOException {
		lineReader.seek(pos);
	}
}
