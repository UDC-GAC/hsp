package es.udc.gac.hadoop.sequence.parser.mapreduce;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import es.udc.gac.hadoop.sequence.parser.TestSupport;

/** What the equivalence tests of the record readers share: how a split is read and how texts are generated. */
final class EquivalenceSupport {

	private EquivalenceSupport() {
	}

	static final int[] BUFFER_SIZES = { 1, 2, 3, 7, 16, 65536 };

	/** Creates the reader under test; warnings counts the calls to warnTruncatedRecord(). */
	interface Factory {
		SingleEndSequenceRecordReader create(TaskAttemptContext ctx, int[] warnings);
	}

	/** What a reader produced for a split: its records, the number of truncated record warnings and the error, if any. */
	static String read(Factory factory, Configuration conf, InputSplit split) {
		TaskAttemptContext ctx = TestSupport.context(conf);
		int[] warnings = { 0 };
		SingleEndSequenceRecordReader reader = factory.create(ctx, warnings);

		StringBuilder sb = new StringBuilder();
		try {
			reader.initialize(split, ctx);
			while (reader.nextKeyValue())
				sb.append(reader.getCurrentKey().get()).append(" [").append(SingleEndSequenceRecordReader.getRead(reader.getCurrentValue())).append("]\n");
		} catch (Exception e) {
			sb.append("ERROR ").append(e.getClass().getName()).append(": ").append(e.getMessage());
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				sb.append("ERROR on close ").append(e);
			}
		}
		return sb.append("warnings=").append(warnings[0]).toString();
	}

	static String read(Factory factory, Configuration conf, Path path, long start, long length) {
		return read(factory, conf, new FileSplit(path, start, length, null));
	}

	static Configuration conf(int bufferSize, boolean trim) {
		Configuration conf = TestSupport.newConf();
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setInputBufferSize(conf, bufferSize);
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setTrimSequenceName(conf, trim);
		return conf;
	}

	static String word(Random rnd, String alphabet, int len) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++)
			sb.append(alphabet.charAt(rnd.nextInt(alphabet.length())));
		return sb.toString();
	}
}
