package es.udc.gac.hadoop.sequence.parser.mapreduce;

import static es.udc.gac.hadoop.sequence.parser.TestSupport.expectedFasta;
import static es.udc.gac.hadoop.sequence.parser.TestSupport.expectedFastq;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import es.udc.gac.hadoop.sequence.parser.TestSupport;
import es.udc.gac.hadoop.sequence.parser.TestSupport.Record;

/** FastQInputFormat / FastAInputFormat: splitting and reading of plain and compressed files. */
public class SingleEndInputFormatTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	private Configuration conf;

	@Before
	public void setUp() {
		conf = TestSupport.newConf();
	}

	private static List<String> values(List<Record> records) {
		List<String> values = new ArrayList<>();
		for (Record r : records)
			values.add(r.value);
		return values;
	}

	// ---------------------------------------------------------------------
	// Splittability
	// ---------------------------------------------------------------------

	@Test
	public void plainAndBzip2FilesAreSplittableButGzipIsNot() throws Exception {
		Job job = Job.getInstance(conf);
		FastQInputFormat format = new FastQInputFormat();

		assertTrue(format.isSplitable(job, new Path("file:///data/reads.fq")));
		assertTrue(format.isSplitable(job, new Path("file:///data/reads.fastq")));
		assertTrue(format.isSplitable(job, new Path("file:///data/reads.fq.bz2")));
		assertFalse(format.isSplitable(job, new Path("file:///data/reads.fq.gz")));
		assertFalse(new FastAInputFormat().isSplitable(job, new Path("file:///data/reads.fa.gz")));
		assertTrue(new FastAInputFormat().isSplitable(job, new Path("file:///data/reads.fa.bz2")));
	}

	@Test
	public void readerTypeMatchesTheFormat() throws Exception {
		FileSplit split = new FileSplit(new Path("file:///x"), 0, 1, null);

		assertTrue(new FastQInputFormat().createRecordReader(split, TestSupport.context(conf)) instanceof FastQRecordReader);
		assertTrue(new FastAInputFormat().createRecordReader(split, TestSupport.context(conf)) instanceof FastARecordReader);
	}

	// ---------------------------------------------------------------------
	// Number of splits
	// ---------------------------------------------------------------------

	@Test
	public void numberOfSplitsMatchesTheSplitsGeneratedByHadoop() throws Exception {
		String text = TestSupport.fastq(200, 1, true);
		long length = text.getBytes(StandardCharsets.UTF_8).length;
		Path path = TestSupport.write(tmp.getRoot(), "reads.fq", text);
		FastQInputFormat format = new FastQInputFormat();

		for (long splitSize : new long[] { 1000, 1024, 4096, 5000, length / 2, length - 1, length, length + 1, 10 * length }) {
			int generated = TestSupport.getSplits(format, conf, path, splitSize).size();
			assertEquals("split size " + splitSize, generated, SingleEndSequenceInputFormat.getNumberOfSplits(path, length, true, splitSize));
		}
	}

	@Test
	public void numberOfSplitsOfNonSplittableOrEmptyFiles() throws Exception {
		Path path = new Path("file:///data/reads.fq.gz");

		assertEquals(1, SingleEndSequenceInputFormat.getNumberOfSplits(path, 1_000_000, false, 1000));
		assertEquals(0, SingleEndSequenceInputFormat.getNumberOfSplits(path, 0, true, 1000));
		assertEquals(0, SingleEndSequenceInputFormat.getNumberOfSplits(path, 0, false, 1000));
	}

	@Test
	public void splitsAreContiguousAndCoverTheWholeFile() throws Exception {
		String text = TestSupport.fastq(200, 2, true);
		long length = text.getBytes(StandardCharsets.UTF_8).length;
		Path path = TestSupport.write(tmp.getRoot(), "reads.fq", text);

		List<InputSplit> splits = TestSupport.getSplits(new FastQInputFormat(), conf, path, 1500);
		long expectedStart = 0;
		for (InputSplit s : splits) {
			FileSplit fs = (FileSplit) s;
			assertEquals(path, fs.getPath());
			assertEquals(expectedStart, fs.getStart());
			expectedStart += fs.getLength();
		}

		assertTrue(splits.size() > 1);
		assertEquals(length, expectedStart);
	}

	// ---------------------------------------------------------------------
	// Plain files: any split size gives the same records
	// ---------------------------------------------------------------------

	@Test
	public void fastqSplitsOfAnySizeGiveTheSameRecords() throws Exception {
		String text = TestSupport.fastq(400, 3, true);
		List<Record> expected = expectedFastq(text, true);
		long length = text.getBytes(StandardCharsets.UTF_8).length;
		Path path = TestSupport.write(tmp.getRoot(), "reads.fq", text);

		for (long splitSize : new long[] { 200, 333, 1000, 4096, 12345, length / 2, length, 2 * length })
			assertEquals("split size " + splitSize, values(expected), values(TestSupport.readAllSplits(FastQInputFormat.class, conf, path, splitSize)));
	}

	@Test
	public void fastaSplitsOfAnySizeGiveTheSameRecords() throws Exception {
		String text = TestSupport.fasta(400, 4, true);
		List<Record> expected = expectedFasta(text, true);
		long length = text.getBytes(StandardCharsets.UTF_8).length;
		Path path = TestSupport.write(tmp.getRoot(), "reads.fa", text);

		for (long splitSize : new long[] { 1000, 4096, 12345, length / 2, length, 2 * length })
			assertEquals("split size " + splitSize, values(expected), values(TestSupport.readAllSplits(FastAInputFormat.class, conf, path, splitSize)));
	}

	@Test
	public void fastqSplitsShorterThanARecordGiveTheSameRecords() throws Exception {
		String text = TestSupport.fastq(400, 3, true);
		Path path = TestSupport.write(tmp.getRoot(), "reads.fq", text);

		for (long splitSize : new long[] { 1, 7, 50, 100, 200 })
			assertEquals("split size " + splitSize, values(expectedFastq(text, true)), values(TestSupport.readAllSplits(FastQInputFormat.class, conf, path, splitSize)));
	}

	@Test
	public void fastaSplitsShorterThanARecordGiveTheSameRecords() throws Exception {
		String text = TestSupport.fasta(400, 4, true);
		Path path = TestSupport.write(tmp.getRoot(), "reads.fa", text);

		for (long splitSize : new long[] { 1, 7, 50, 200, 333 })
			assertEquals("split size " + splitSize, values(expectedFasta(text, true)), values(TestSupport.readAllSplits(FastAInputFormat.class, conf, path, splitSize)));
	}

	@Test
	public void fastqSplitsStartingAtOrAfterTheLastRecordAreHandled() throws Exception {
		String text = TestSupport.fastq(10, 5, true);
		List<Record> expected = expectedFastq(text, true);
		long length = text.getBytes(StandardCharsets.UTF_8).length;
		Path path = TestSupport.write(tmp.getRoot(), "reads.fq", text);

		// A tiny last split: it only contains the end of the last record
		long lastKey = expected.get(expected.size() - 1).key;
		List<Record> all = new ArrayList<>(TestSupport.readSplit(FastQInputFormat.class, conf, path, 0, lastKey + 3));
		all.addAll(TestSupport.readSplit(FastQInputFormat.class, conf, path, lastKey + 3, length - lastKey - 3));

		assertEquals(values(expected), values(all));
	}

	@Test
	public void emptyFileGivesNoRecords() throws Exception {
		Path path = TestSupport.write(tmp.getRoot(), "empty.fq", "");

		assertTrue(TestSupport.readAllSplits(FastQInputFormat.class, conf, path, 1000).isEmpty());
		assertTrue(TestSupport.readAllSplits(FastAInputFormat.class, conf, path, 1000).isEmpty());
	}

	// ---------------------------------------------------------------------
	// Compressed files
	// ---------------------------------------------------------------------

	@Test
	public void gzipFastqIsReadInOneSplit() throws Exception {
		String text = TestSupport.fastq(400, 6, true);
		byte[] gz = TestSupport.compress(GzipCodec.class, conf, text.getBytes(StandardCharsets.UTF_8));
		Path path = TestSupport.write(tmp.getRoot(), "reads.fq.gz", gz);

		List<InputSplit> splits = TestSupport.getSplits(new FastQInputFormat(), conf, path, 100);
		assertEquals(1, splits.size());
		assertEquals(values(expectedFastq(text, true)), values(TestSupport.readAllSplits(FastQInputFormat.class, conf, path, 100)));
	}

	@Test
	public void gzipSmallFastaIsReadInOneSplit() throws Exception {
		String text = TestSupport.fasta(5, 7, true);
		byte[] gz = TestSupport.compress(GzipCodec.class, conf, text.getBytes(StandardCharsets.UTF_8));
		Path path = TestSupport.write(tmp.getRoot(), "small.fa.gz", gz);

		assertEquals(1, TestSupport.getSplits(new FastAInputFormat(), conf, path, 100).size());
		assertEquals(values(expectedFasta(text, true)), values(TestSupport.readAllSplits(FastAInputFormat.class, conf, path, 100)));
	}

	@Test
	public void gzipFastaIsReadInOneSplit() throws Exception {
		String text = TestSupport.fasta(400, 7, true);
		byte[] gz = TestSupport.compress(GzipCodec.class, conf, text.getBytes(StandardCharsets.UTF_8));
		Path path = TestSupport.write(tmp.getRoot(), "reads.fa.gz", gz);

		assertEquals(1, TestSupport.getSplits(new FastAInputFormat(), conf, path, 100).size());
		assertEquals(values(expectedFasta(text, true)), values(TestSupport.readAllSplits(FastAInputFormat.class, conf, path, 100)));
	}

	@Test
	public void gzipSplitNotStartingAtTheBeginningIsRejected() throws Exception {
		byte[] gz = TestSupport.compress(GzipCodec.class, conf, TestSupport.fastq(50, 8, true).getBytes(StandardCharsets.UTF_8));
		Path path = TestSupport.write(tmp.getRoot(), "reads.fq.gz", gz);

		try {
			TestSupport.readSplit(FastQInputFormat.class, conf, path, 10, gz.length - 10);
			fail("expected IOException");
		} catch (IOException expected) {
			assertTrue(expected.getMessage(), expected.getMessage().contains("Cannot seek in"));
		}
	}

	@Test
	public void bzip2FastqSingleSplitGivesAllRecords() throws Exception {
		String text = TestSupport.fastq(400, 9, true);
		byte[] bz2 = TestSupport.compress(BZip2Codec.class, conf, text.getBytes(StandardCharsets.UTF_8));
		Path path = TestSupport.write(tmp.getRoot(), "reads.fq.bz2", bz2);

		assertEquals(expectedFastq(text, true), TestSupport.readWhole(FastQInputFormat.class, conf, path, bz2.length));
	}

	@Test
	public void bzip2SmallFastaSingleSplitGivesAllRecords() throws Exception {
		String text = TestSupport.fasta(5, 10, true);
		byte[] bz2 = TestSupport.compress(BZip2Codec.class, conf, text.getBytes(StandardCharsets.UTF_8));
		Path path = TestSupport.write(tmp.getRoot(), "small.fa.bz2", bz2);

		assertEquals(values(expectedFasta(text, true)), values(TestSupport.readWhole(FastAInputFormat.class, conf, path, bz2.length)));
	}

	@Test
	public void bzip2FastaSingleSplitGivesAllRecords() throws Exception {
		String text = TestSupport.fasta(400, 10, true);
		byte[] bz2 = TestSupport.compress(BZip2Codec.class, conf, text.getBytes(StandardCharsets.UTF_8));
		Path path = TestSupport.write(tmp.getRoot(), "reads.fa.bz2", bz2);

		assertEquals(values(expectedFasta(text, true)), values(TestSupport.readWhole(FastAInputFormat.class, conf, path, bz2.length)));
	}

	/** Large enough to span several bzip2 blocks (900 KB of uncompressed data each). */
	@Test
	public void bzip2FastqSplitsOfAnySizeGiveTheSameRecords() throws Exception {
		String text = TestSupport.fastq(30000, 11, true);
		byte[] bz2 = TestSupport.compress(BZip2Codec.class, conf, text.getBytes(StandardCharsets.UTF_8));
		Path path = TestSupport.write(tmp.getRoot(), "big.fq.bz2", bz2);
		List<String> expected = values(expectedFastq(text, true));

		for (long splitSize : new long[] { 100_000, 250_000, 600_000, bz2.length / 2, bz2.length })
			assertEquals("split size " + splitSize, expected, values(TestSupport.readAllSplits(FastQInputFormat.class, conf, path, splitSize)));
	}

	@Test
	public void bzip2FastaSplitsOfAnySizeGiveTheSameRecords() throws Exception {
		String text = TestSupport.fasta(30000, 12, true);
		byte[] bz2 = TestSupport.compress(BZip2Codec.class, conf, text.getBytes(StandardCharsets.UTF_8));
		Path path = TestSupport.write(tmp.getRoot(), "big.fa.bz2", bz2);
		List<String> expected = values(expectedFasta(text, true));

		for (long splitSize : new long[] { 100_000, 250_000, 600_000, bz2.length / 2, bz2.length })
			assertEquals("split size " + splitSize, expected, values(TestSupport.readAllSplits(FastAInputFormat.class, conf, path, splitSize)));
	}
}
