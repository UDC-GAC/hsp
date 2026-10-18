package es.udc.gac.hadoop.sequence.parser.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import es.udc.gac.hadoop.sequence.parser.TestSupport;

/**
 * Compares FastQRecordReader with the reader it replaced (LegacyFastQRecordReader) on well-formed and
 * malformed FASTQ text, cut into splits at random places and read through buffers of many sizes: records,
 * keys, warnings and exceptions must be the same.
 */
public class FastQReaderEquivalenceTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	private static final int[] BUFFER_SIZES = { 1, 2, 3, 7, 16, 65536 };

	/** What a reader produced for a split: its records, the number of truncated record warnings and the error, if any. */
	private static String read(boolean legacy, Configuration conf, Path path, long start, long length) {
		TaskAttemptContext ctx = TestSupport.context(conf);
		int[] warnings = { 0 };
		SingleEndSequenceRecordReader reader = legacy ? new LegacyFastQRecordReader(ctx) {
			@Override
			protected void warnTruncatedRecord() {
				warnings[0]++;
			}
		} : new FastQRecordReader(ctx) {
			@Override
			protected void warnTruncatedRecord() {
				warnings[0]++;
			}
		};

		StringBuilder sb = new StringBuilder();
		try {
			reader.initialize(new FileSplit(path, start, length, null), ctx);
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

	private static Configuration conf(int bufferSize, boolean trim) {
		Configuration conf = TestSupport.newConf();
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setInputBufferSize(conf, bufferSize);
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setTrimSequenceName(conf, trim);
		return conf;
	}

	private static void sameOutput(String what, Configuration conf, Path path, long start, long length) {
		assertEquals(what, read(true, conf, path, start, length), read(false, conf, path, start, length));
	}

	private static String word(Random rnd, String alphabet, int len) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++)
			sb.append(alphabet.charAt(rnd.nextInt(alphabet.length())));
		return sb.toString();
	}

	private static String randomLine(Random rnd) {
		return word(rnd, "@+ACGT# ", 1) + word(rnd, "@+ACGT #", rnd.nextInt(7)) + "\n";
	}

	/** FASTQ records with the odd things that real and damaged files contain, or just random lines. */
	static String randomText(Random rnd) {
		StringBuilder sb = new StringBuilder();

		if (rnd.nextInt(10) < 3) {
			for (int n = rnd.nextInt(14); n > 0; n--)
				sb.append(randomLine(rnd));
		} else {
			for (int n = rnd.nextInt(7); n > 0; n--) {
				String header = "@" + word(rnd, "abc12:/.", 1 + rnd.nextInt(5)) + (rnd.nextBoolean() ? " " + word(rnd, "xy z=", rnd.nextInt(6)) : "");
				String sequence = word(rnd, "ACGTN", rnd.nextInt(8));
				String plus = rnd.nextBoolean() ? "+" : "+" + header.substring(1);
				String quality = (rnd.nextInt(4) == 0 ? "@" : "") + word(rnd, "!#5@I+", rnd.nextInt(8));

				switch (rnd.nextInt(10)) {
				case 6: // other lines between records
					sb.append(randomLine(rnd));
					break;
				case 7: // truncated record
					String[] lines = { header, sequence, plus, quality };
					for (int l = rnd.nextInt(4); l >= 0; l--)
						sb.append(lines[3 - l]).append(l == 0 && rnd.nextBoolean() ? "" : "\n");
					break;
				case 8: // sequence starting with '@'
					sequence = "@" + sequence;
					break;
				case 9: // comment line starting with '@'
					plus = "@" + plus;
					break;
				default:
				}

				if (!sb.toString().endsWith("\n") && sb.length() > 0)
					sb.append('\n');
				sb.append(header).append('\n').append(sequence).append('\n').append(plus).append('\n').append(quality).append('\n');
			}
		}

		if (sb.length() > 0 && rnd.nextInt(4) == 0)
			sb.setLength(sb.length() - 1); // no line feed at the end
		return sb.toString();
	}

	@Test
	public void sameRecordsOnPlainFilesCutAnywhere() throws IOException {
		Random rnd = new Random(2026);

		for (int round = 0; round < 600; round++) {
			String text = randomText(rnd);
			byte[] data = text.getBytes(StandardCharsets.US_ASCII);
			Path path = TestSupport.write(tmp.getRoot(), "r" + round + ".fq", data);
			long len = data.length;

			List<long[]> splits = new ArrayList<>();
			splits.add(new long[] { 0, len });
			for (int s = 0; s < 3; s++) {
				long start = len == 0 ? 0 : rnd.nextInt((int) len + 1);
				splits.add(new long[] { start, rnd.nextInt((int) (len - start) + 4) });
			}

			for (int bufferSize : new int[] { BUFFER_SIZES[rnd.nextInt(BUFFER_SIZES.length)], BUFFER_SIZES[rnd.nextInt(BUFFER_SIZES.length)] }) {
				for (boolean trim : new boolean[] { true, false }) {
					Configuration conf = conf(bufferSize, trim);
					for (long[] split : splits)
						sameOutput("round " + round + ", buffer " + bufferSize + ", trim " + trim + ", split " + split[0] + "+" + split[1] + ", text:\n" + text,
								conf, path, split[0], split[1]);
				}
			}
		}
	}

	@Test
	public void sameRecordsOnCompressedFiles() throws IOException {
		Random rnd = new Random(7);

		for (int round = 0; round < 80; round++) {
			String text = randomText(rnd);
			byte[] data = text.getBytes(StandardCharsets.US_ASCII);
			Configuration base = TestSupport.newConf();
			Path gz = TestSupport.write(tmp.getRoot(), "r" + round + ".fq.gz", TestSupport.compress(GzipCodec.class, base, data));
			Path bz = TestSupport.write(tmp.getRoot(), "r" + round + ".fq.bz2", TestSupport.compress(BZip2Codec.class, base, data));

			for (int bufferSize : new int[] { 1, 5, 65536 }) {
				for (boolean trim : new boolean[] { true, false }) {
					Configuration conf = conf(bufferSize, trim);
					String what = "round " + round + ", buffer " + bufferSize + ", trim " + trim + ", text:\n" + text;
					sameOutput("gzip, " + what, conf, gz, 0, Long.MAX_VALUE / 2);
					sameOutput("bzip2, " + what, conf, bz, 0, Long.MAX_VALUE / 2);
				}
			}
		}
	}
}
