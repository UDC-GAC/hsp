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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Text-based InputFormat for paired-end sequence files.
 * 
 * @author Roberto Rey Exposito		<rreye@udc.es>
 * @author Luis Lorenzo Mosquera	<luis.lorenzom@udc.es>
 * @author Jorge González-Domínguez	<jgonzalezd@udc.es> 
 */
public class PairedEndSequenceInputFormat extends FileInputFormat<LongWritable, PairText> {

	private static final Logger logger = LogManager.getLogger();
	private static final double SPLIT_SLOP = 1.1; // 10% slop

	public static final String LEFT_INPUT_PATH = "hsra.paired.left.path";
	public static final String RIGHT_INPUT_PATH = "hsra.paired.right.path";
	public static final String LEFT_INPUT_FORMAT = "hsra.paired.left.inputformat";
	public static final String RIGHT_INPUT_FORMAT = "hsra.paired.right.inputformat";

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
	 * @param if the file can be splitable
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

	/**
	 * 
	 * @param The job submitter's view
	 * @param The input Path
	 * @param The inputFormat class required by inputPath
	 * @throws IOException if there is any problem in the I/O file's operations
	 */
	public static void setLeftInputPath(Job job, Path inputPath, Class<? extends SingleEndSequenceInputFormat> inputFormatClass) throws IOException {
		Configuration conf = job.getConfiguration();
		checkInputPath(conf, inputPath);
		Path path = inputPath.getFileSystem(conf).makeQualified(inputPath);
		conf.set(LEFT_INPUT_FORMAT, inputFormatClass.getCanonicalName());
		conf.set(LEFT_INPUT_PATH, StringUtils.escapeString(path.toString()));
	}

	/**
	 * 
	 * @param The job configuration
	 * @param The input Path
	 * @param The inputFormat class required by inputPath
	 * @throws IOException if there is any problem in the I/O file's operations
	 */
	public static void setLeftInputPath(Configuration conf, Path inputPath, Class<? extends SingleEndSequenceInputFormat> inputFormatClass) throws IOException {
		checkInputPath(conf, inputPath);
		Path path = inputPath.getFileSystem(conf).makeQualified(inputPath);
		conf.set(LEFT_INPUT_FORMAT, inputFormatClass.getCanonicalName());
		conf.set(LEFT_INPUT_PATH, StringUtils.escapeString(path.toString()));
	}

	/**
	 * 
	 * @param The job submitter's view
	 * @param The input Path
	 * @param The inputFormat class required by inputPath
	 * @throws IOException if there is any problem in the I/O file's operations
	 */
	public static void setRightInputPath(Job job, Path inputPath, Class<? extends SingleEndSequenceInputFormat> inputFormatClass) throws IOException {
		Configuration conf = job.getConfiguration();
		checkInputPath(conf, inputPath);
		Path path = inputPath.getFileSystem(conf).makeQualified(inputPath);
		conf.set(RIGHT_INPUT_FORMAT, inputFormatClass.getCanonicalName());
		conf.set(RIGHT_INPUT_PATH, StringUtils.escapeString(path.toString()));
	}

	/**
	 * 
	 * @param The job configuration
	 * @param The input Path
	 * @param The inputFormat class required by inputPath
	 * @throws IOException if there is any problem in the I/O file's operations
	 */
	public static void setRightInputPath(Configuration conf, Path inputPath, Class<? extends SingleEndSequenceInputFormat> inputFormatClass) throws IOException {
		checkInputPath(conf, inputPath);
		Path path = inputPath.getFileSystem(conf).makeQualified(inputPath);
		conf.set(RIGHT_INPUT_FORMAT, inputFormatClass.getCanonicalName());
		conf.set(RIGHT_INPUT_PATH, StringUtils.escapeString(path.toString()));
	}

	/**
	 * 
	 * @param The job's context
	 * @return A Path array with all inputPaths
	 */
	public static Path[] getInputPaths(JobContext context) {
		String leftInputPath = context.getConfiguration().get(LEFT_INPUT_PATH, "");
		String rightInputPath = context.getConfiguration().get(RIGHT_INPUT_PATH, "");
		Path[] paths = new Path[2];
		paths[0] = new Path(StringUtils.unEscapeString(leftInputPath));
		paths[1] = new Path(StringUtils.unEscapeString(rightInputPath));
		return paths;
	}

	/**
	 * 
	 * @param The job's context
	 * @return A string array with all inputFormats
	 */
	public static String[] getInputFormats(JobContext context) {
		String leftInputFormat = context.getConfiguration().get(LEFT_INPUT_FORMAT, "");
		String rightInputFormat = context.getConfiguration().get(RIGHT_INPUT_FORMAT, "");
		String[] inputFormats = new String[2];
		inputFormats[0] = leftInputFormat;
		inputFormats[1] = rightInputFormat;
		return inputFormats;
	}

	@Override
	public RecordReader<LongWritable, PairText> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		return new PairedEndSequenceRecordReader((PairedEndInputSplit) split, context);
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		// Get input paths
		Path[] inputPaths = getInputPaths(job);

		if (inputPaths.length == 0)
			throw new IOException("No input paths specified in job");

		// Get input formats
		String[] inputFormats = getInputFormats(job);

		if (inputFormats.length == 0)
			throw new IOException("No input formats specified in job");

		Path leftPath = inputPaths[0];
		Path rightPath = inputPaths[1];
		String leftInputFormat = inputFormats[0];
		String rightInputFormat = inputFormats[1];

		if (!leftInputFormat.equals(rightInputFormat))
			throw new IOException("Input formats do not match: "+leftInputFormat+", "+rightInputFormat);

		// Generate file splits for both input paths
		List<InputSplit> leftSplits;
		List<InputSplit> rightSplits;
		try {
			leftSplits = getInputSplits(job, leftPath, leftInputFormat);
			rightSplits = getInputSplits(job, rightPath, rightInputFormat);
			if (logger.isDebugEnabled()) {
				logger.debug(leftSplits);
				logger.debug(rightSplits);
			}
		} catch (ClassNotFoundException e) {
			throw new IOException(e.getMessage());
		}

		if(leftSplits.size() == 0)
			throw new IOException("No file splits have been generated for input path "+leftPath);

		if(rightSplits.size() == 0)
			throw new IOException("No file splits have been generated for input path "+rightPath);

		if(leftSplits.size() != rightSplits.size())
			throw new IOException("Number of file splits does not match: "+leftSplits.size()+","+rightSplits.size());

		// Generate composite input splits
		List<InputSplit> splits = new ArrayList<InputSplit>(leftSplits.size());

		try {
			for(int i = 0; i<leftSplits.size(); i++) {
				PairedEndInputSplit pairedSplit = new PairedEndInputSplit();
				pairedSplit.add((FileSplit)leftSplits.get(i));
				pairedSplit.add((FileSplit)rightSplits.get(i));
				splits.add(pairedSplit);
			}
		} catch (InterruptedException e) {
			throw new IOException(e.getMessage());
		}

		return splits;
	}

	/**
	 * Get the a list of InputSplits for any inputPath.
	 * 
	 * @param The job submitter's view
	 * @param The input Path
	 * @param The inputFormat class required by inputPath
	 * @return A list which contents all the inputSplits
	 * @throws ClassNotFoundException if the inputFormatClass cannot be instance
	 * @throws IOException if there is any problem in the I/O file's operations
	 */
	private List<InputSplit> getInputSplits(JobContext job, Path inputPath, String inputFormatClass) throws ClassNotFoundException, IOException {
		Configuration conf = job.getConfiguration();

		// Create a new instance of the input format
		SingleEndSequenceInputFormat inputFormat = (SingleEndSequenceInputFormat)
				ReflectionUtils.newInstance(Class.forName(inputFormatClass), conf);

		// Add input path
		Path path = inputPath.getFileSystem(conf).makeQualified(inputPath);
		String str = StringUtils.escapeString(path.toString());
		conf.set(INPUT_DIR, str);

		// Get input splits
		return inputFormat.getSplits(job);
	}

	/**
	 * Check if the input has any problem.
	 * 
	 * @param The job configuration
	 * @param The input Path
	 * @throws IOException for any problem found
	 */
	private static void checkInputPath(Configuration conf, Path inputPath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] contents = fs.listStatus(inputPath);

		if (contents == null) {
			throw new IOException("Input path does not exist: "+inputPath);
		} else if (contents.length != 1) {
			throw new IOException("Input path ("+inputPath+") matches multiple files");
		} else {
			FileStatus file = contents[0];

			if (!file.isFile())
				throw new IOException("Input path ("+inputPath+") must be a sequence file in FASTQ/FASTA format");

			if (file.getLen() == 0)
				throw new IOException("Input path ("+inputPath+") is empty");
		}
	}
}
