package es.udc.gac.hadoop.sequence.parser.mapreduce;

import static es.udc.gac.hadoop.sequence.parser.mapreduce.EquivalenceSupport.BUFFER_SIZES;
import static es.udc.gac.hadoop.sequence.parser.mapreduce.EquivalenceSupport.conf;
import static es.udc.gac.hadoop.sequence.parser.mapreduce.EquivalenceSupport.read;
import static es.udc.gac.hadoop.sequence.parser.mapreduce.EquivalenceSupport.word;
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
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import es.udc.gac.hadoop.sequence.parser.TestSupport;
import es.udc.gac.hadoop.sequence.parser.mapreduce.EquivalenceSupport.Factory;

/**
 * Compares FastARecordReader with the reader it replaced (LegacyFastARecordReader) on well-formed and
 * malformed FASTA text, cut into splits at random places and read through buffers of many sizes: records,
 * keys and exceptions must be the same.
 */
public class FastAReaderEquivalenceTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	private static final Factory LEGACY = (ctx, warnings) -> new LegacyFastARecordReader(ctx);
	private static final Factory CURRENT = (ctx, warnings) -> new FastARecordReader(ctx);

	private static void sameOutput(String what, Configuration conf, Path path, long start, long length) {
		assertEquals(what, read(LEGACY, conf, path, start, length), read(CURRENT, conf, path, start, length));
	}

	private static String randomLine(Random rnd) {
		return word(rnd, ">ACGT# ", 1) + word(rnd, ">ACGT #", rnd.nextInt(7)) + "\n";
	}

	/** FASTA records with the odd things that real and damaged files contain, or just random lines. */
	static String randomText(Random rnd) {
		StringBuilder sb = new StringBuilder();

		if (rnd.nextInt(10) < 3) {
			for (int n = rnd.nextInt(14); n > 0; n--)
				sb.append(randomLine(rnd));
		} else {
			if (rnd.nextInt(5) == 0) // lines before the first header
				sb.append(word(rnd, "ACGT ", rnd.nextInt(6))).append('\n');

			for (int n = rnd.nextInt(7); n > 0; n--) {
				sb.append('>').append(word(rnd, "abc12:/.", rnd.nextInt(5))).append(rnd.nextBoolean() ? " " + word(rnd, "xy z=", rnd.nextInt(6)) : "").append('\n');

				for (int l = rnd.nextInt(5); l > 0; l--) {
					switch (rnd.nextInt(8)) {
					case 0: // blank line
						sb.append('\n');
						break;
					case 1: // line starting with a space
						sb.append(' ').append(word(rnd, "ACGT", rnd.nextInt(6))).append('\n');
						break;
					default:
						sb.append(word(rnd, "ACGTN", 1 + rnd.nextInt(8))).append('\n');
					}
				}
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
			Path path = TestSupport.write(tmp.getRoot(), "r" + round + ".fa", data);
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
			Path gz = TestSupport.write(tmp.getRoot(), "r" + round + ".fa.gz", TestSupport.compress(GzipCodec.class, base, data));
			Path bz = TestSupport.write(tmp.getRoot(), "r" + round + ".fa.bz2", TestSupport.compress(BZip2Codec.class, base, data));

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

	/** Large enough to span several bzip2 blocks: the split ends are decided on the position of the compressed stream. */
	@Test
	public void sameRecordsOnBzip2SplitsOfSeveralBlocks() throws IOException {
		Configuration conf = TestSupport.newConf();
		byte[] data = TestSupport.fasta(30000, 12, true).getBytes(StandardCharsets.US_ASCII);
		byte[] bz2 = TestSupport.compress(BZip2Codec.class, conf, data);
		Path path = TestSupport.write(tmp.getRoot(), "big.fa.bz2", bz2);

		for (int bufferSize : new int[] { 5, 65536 }) {
			Configuration c = conf(bufferSize, true);
			for (long splitSize : new long[] { 100_000, 250_000, 600_000 }) {
				for (InputSplit split : TestSupport.getSplits(new FastAInputFormat(), c, path, splitSize))
					assertEquals("buffer " + bufferSize + ", split size " + splitSize + ", split " + split, read(LEGACY, c, split), read(CURRENT, c, split));
			}
		}
	}
}
