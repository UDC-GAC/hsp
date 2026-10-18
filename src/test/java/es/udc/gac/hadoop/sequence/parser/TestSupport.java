package es.udc.gac.hadoop.sequence.parser;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

import es.udc.gac.hadoop.sequence.parser.mapreduce.PairText;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndInputSplit;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndSequenceInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndSequenceRecordReader;
import es.udc.gac.hadoop.sequence.parser.mapreduce.SingleEndSequenceInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.SingleEndSequenceRecordReader;

/**
 * Helpers shared by the tests: synthetic FASTQ/FASTA generation, file writing,
 * and reading of splits through the real InputFormats/RecordReaders.
 */
public final class TestSupport {

	private TestSupport() {
	}

	/** A key/value pair emitted by a single-end record reader. */
	public static final class Record {
		public final long key;
		public final String value;

		public Record(long key, String value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Record))
				return false;
			Record r = (Record) o;
			return key == r.key && value.equals(r.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, value);
		}

		@Override
		public String toString() {
			return key + "=" + value.replace("\n", "\\n");
		}
	}

	/** A key/value pair emitted by a paired-end record reader. */
	public static final class PairRecord {
		public final long key;
		public final String left;
		public final String right;

		public PairRecord(long key, String left, String right) {
			this.key = key;
			this.left = left;
			this.right = right;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof PairRecord))
				return false;
			PairRecord r = (PairRecord) o;
			return key == r.key && left.equals(r.left) && right.equals(r.right);
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, left, right);
		}

		@Override
		public String toString() {
			return key + "=(" + left.replace("\n", "\\n") + " | " + right.replace("\n", "\\n") + ")";
		}
	}

	// ---------------------------------------------------------------------
	// Synthetic data
	// ---------------------------------------------------------------------

	/**
	 * Builds a FASTQ file. Every record has a header with a comment (space-separated), a
	 * '+' line repeating the name, and qualities which may start with '@'.
	 */
	public static String fastq(int numReads, long seed, boolean trailingNewline) {
		return fastq(numReads, seed, "r", trailingNewline);
	}

	public static String fastq(int numReads, long seed, String prefix, boolean trailingNewline) {
		Random rnd = new Random(seed);
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < numReads; i++) {
			int len = 1 + rnd.nextInt(60);
			sb.append('@').append(prefix).append(i + 1).append(" comment:").append(i).append(" len=").append(len).append('\n');
			sb.append(randomString(rnd, "ACGTN", len)).append('\n');
			sb.append(i % 2 == 0 ? "+" : "+" + prefix + (i + 1)).append('\n');
			// Qualities start with '@' roughly 1 in 4 times (valid FASTQ, ambiguous with a header)
			String qual = randomString(rnd, "!#$%&()*+,-./0123456789:;<=>?@ABCDEFGHIJ", len);
			if (rnd.nextInt(4) == 0)
				qual = "@" + qual.substring(1);
			sb.append(qual);
			if (i < numReads - 1 || trailingNewline)
				sb.append('\n');
		}

		return sb.toString();
	}

	/** Builds a FASTA file with multi-line sequences. */
	public static String fasta(int numReads, long seed, boolean trailingNewline) {
		Random rnd = new Random(seed);
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < numReads; i++) {
			sb.append(">seq").append(i + 1).append(" description ").append(i).append('\n');
			int lines = 1 + rnd.nextInt(4);
			for (int l = 0; l < lines; l++) {
				sb.append(randomString(rnd, "ACGT", 1 + rnd.nextInt(60)));
				if (i < numReads - 1 || l < lines - 1 || trailingNewline)
					sb.append('\n');
			}
		}

		return sb.toString();
	}

	private static String randomString(Random rnd, String alphabet, int len) {
		char[] c = new char[len];
		for (int i = 0; i < len; i++)
			c[i] = alphabet.charAt(rnd.nextInt(alphabet.length()));
		return new String(c);
	}

	// ---------------------------------------------------------------------
	// Expected output model (what the readers are specified to emit)
	// ---------------------------------------------------------------------

	/**
	 * Expected records for a well-formed FASTQ text: the key is the byte offset of the record, the value
	 * has the header optionally cut at its first space and the '+' line stripped of any comment.
	 */
	public static List<Record> expectedFastq(String text, boolean trimName) {
		List<Record> records = new ArrayList<>();
		List<String> lines = lines(text);
		long offset = 0;

		for (int i = 0; i < lines.size(); i += 4) {
			String header = lines.get(i);
			String hdr = trimName ? cutAtSpace(header, 2) : header;
			StringBuilder value = new StringBuilder(hdr).append(lines.get(i + 1)).append("+\n").append(lines.get(i + 3));
			records.add(new Record(offset, value.toString()));
			offset += header.length() + lines.get(i + 1).length() + lines.get(i + 2).length() + lines.get(i + 3).length();
		}

		return records;
	}

	/** Expected records for a well-formed FASTA text (see {@link #expectedFastq}). */
	public static List<Record> expectedFasta(String text, boolean trimName) {
		List<Record> records = new ArrayList<>();
		long offset = 0, recordStart = 0;
		StringBuilder value = null;

		for (String line : lines(text)) {
			if (line.charAt(0) == '>') {
				if (value != null)
					records.add(new Record(recordStart, value.toString()));
				recordStart = offset;
				value = new StringBuilder(trimName ? cutAtSpace(line, 1) : line);
			} else {
				value.append(line);
			}
			offset += line.length();
		}

		if (value != null)
			records.add(new Record(recordStart, value.toString()));

		return records;
	}

	/** Splits text in lines keeping each line terminator (the last line may have none). */
	private static List<String> lines(String text) {
		List<String> lines = new ArrayList<>();
		int start = 0;
		while (start < text.length()) {
			int nl = text.indexOf('\n', start);
			int end = nl == -1 ? text.length() : nl + 1;
			lines.add(text.substring(start, end));
			start = end;
		}
		return lines;
	}

	/** Mimics LineReader.trim(): the line is cut at the first space found from 'from' and ends with LF. */
	private static String cutAtSpace(String line, int from) {
		int pos = line.indexOf(' ', from);
		return pos == -1 ? line : line.substring(0, pos) + "\n";
	}

	// ---------------------------------------------------------------------
	// Files
	// ---------------------------------------------------------------------

	public static Path write(File dir, String name, String content) throws IOException {
		return write(dir, name, content.getBytes(StandardCharsets.UTF_8));
	}

	public static Path write(File dir, String name, byte[] content) throws IOException {
		File f = new File(dir, name);
		Files.write(f.toPath(), content);
		return new Path(f.toURI());
	}

	public static byte[] compress(Class<? extends CompressionCodec> codecClass, Configuration conf, byte[] data) throws IOException {
		CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (OutputStream out = codec.createOutputStream(bytes)) {
			out.write(data);
		}
		return bytes.toByteArray();
	}

	// ---------------------------------------------------------------------
	// Reading
	// ---------------------------------------------------------------------

	public static Configuration newConf() {
		// Not loading the default resources keeps the tests fast: the readers copy the configuration for each task context
		Configuration conf = new Configuration(false);
		conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec");
		// Most tests were written for trimmed names; the tests of the default (not trimmed) create their own configuration
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setTrimSequenceName(conf, true);
		return conf;
	}

	public static TaskAttemptContext context(Configuration conf) {
		return new TaskAttemptContextImpl(conf, new TaskAttemptID());
	}

	/** Splits a file through FileInputFormat.getSplits() using the given maximum split size. */
	public static List<InputSplit> getSplits(SingleEndSequenceInputFormat format, Configuration conf, Path path, long maxSplitSize) throws IOException {
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, path);
		FileInputFormat.setMaxInputSplitSize(job, maxSplitSize);
		return format.getSplits(job);
	}

	public static List<Record> readSplit(SingleEndSequenceInputFormat format, Configuration conf, InputSplit split) throws IOException, InterruptedException {
		TaskAttemptContext ctx = context(conf);
		List<Record> records = new ArrayList<>();
		RecordReader<LongWritable, Text> reader = format.createRecordReader(split, ctx);
		try {
			reader.initialize(split, ctx);
			while (reader.nextKeyValue())
				records.add(new Record(reader.getCurrentKey().get(), SingleEndSequenceRecordReader.getRead(reader.getCurrentValue())));
		} finally {
			reader.close();
		}
		return records;
	}

	public static List<Record> readSplit(Class<? extends SingleEndSequenceInputFormat> formatClass, Configuration conf, Path path, long start, long length)
			throws IOException, InterruptedException {
		return readSplit(ReflectionUtils.newInstance(formatClass, conf), conf, new FileSplit(path, start, length, null));
	}

	/** Reads all the records of a file as a single split covering the whole file. */
	public static List<Record> readWhole(Class<? extends SingleEndSequenceInputFormat> formatClass, Configuration conf, Path path, long fileLength)
			throws IOException, InterruptedException {
		return readSplit(formatClass, conf, path, 0, fileLength);
	}

	/** Reads and concatenates all the splits generated by getSplits() with the given maximum split size. */
	public static List<Record> readAllSplits(Class<? extends SingleEndSequenceInputFormat> formatClass, Configuration conf, Path path, long maxSplitSize)
			throws IOException, InterruptedException {
		SingleEndSequenceInputFormat format = ReflectionUtils.newInstance(formatClass, conf);
		List<Record> records = new ArrayList<>();
		for (InputSplit split : getSplits(format, conf, path, maxSplitSize))
			records.addAll(readSplit(format, conf, split));
		return records;
	}

	public static List<PairRecord> readPairedSplit(Configuration conf, InputSplit split) throws IOException, InterruptedException {
		TaskAttemptContext ctx = context(conf);
		PairedEndSequenceInputFormat format = new PairedEndSequenceInputFormat();
		List<PairRecord> records = new ArrayList<>();
		RecordReader<LongWritable, PairText> reader = format.createRecordReader(split, ctx);
		try {
			reader.initialize(split, ctx);
			while (reader.nextKeyValue())
				records.add(new PairRecord(reader.getCurrentKey().get(),
						PairedEndSequenceRecordReader.getLeftRead(reader.getCurrentValue()),
						PairedEndSequenceRecordReader.getRightRead(reader.getCurrentValue())));
		} finally {
			reader.close();
		}
		return records;
	}

	public static List<PairRecord> readAllPairedSplits(Configuration conf, long maxSplitSize) throws IOException, InterruptedException {
		Job job = Job.getInstance(conf);
		FileInputFormat.setMaxInputSplitSize(job, maxSplitSize);
		List<PairRecord> records = new ArrayList<>();
		for (InputSplit split : new PairedEndSequenceInputFormat().getSplits(job))
			records.addAll(readPairedSplit(job.getConfiguration(), split));
		return records;
	}

	public static PairedEndInputSplit pairedSplit(FileSplit left, FileSplit right) throws IOException, InterruptedException {
		PairedEndInputSplit split = new PairedEndInputSplit();
		split.add(left);
		split.add(right);
		return split;
	}
}
