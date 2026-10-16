package es.udc.gac.hadoop.sequence.parser.mapreduce;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.VersionInfo;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import es.udc.gac.hadoop.sequence.parser.TestSupport;
import es.udc.gac.hadoop.sequence.parser.TestSupport.PairRecord;
import es.udc.gac.hadoop.sequence.parser.TestSupport.Record;

/** PairedEndSequenceInputFormat and PairedEndSequenceRecordReader. */
public class PairedEndSequenceInputFormatTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	private Configuration conf;

	@Before
	public void setUp() {
		conf = TestSupport.newConf();
	}

	/** The mate of a FASTQ text: same layout (names, lengths) but complemented bases. */
	private static String mateOfFastq(String fastq) {
		StringBuilder sb = new StringBuilder();
		String[] lines = fastq.split("\n", -1);
		for (int i = 0; i < lines.length; i++) {
			String line = lines[i];
			if (i % 4 == 1)
				line = line.replace('A', 't').replace('C', 'g').replace('G', 'c').replace('T', 'a').toUpperCase();
			sb.append(line);
			if (i < lines.length - 1)
				sb.append('\n');
		}
		return sb.toString();
	}

	/** A mate with the same layout but a different compressed size: bases are mapped to fewer symbols. */
	private static String lowEntropyMateOfFastq(String fastq) {
		String[] lines = fastq.split("\n", -1);
		for (int i = 1; i < lines.length; i += 4)
			lines[i] = lines[i].replaceAll("[ACG]", "T");
		return String.join("\n", lines);
	}

	private static String mateOfFasta(String fasta) {
		StringBuilder sb = new StringBuilder();
		String[] lines = fasta.split("\n", -1);
		for (int i = 0; i < lines.length; i++) {
			String line = lines[i];
			if (!line.startsWith(">"))
				line = line.replace('A', 't').replace('C', 'g').replace('G', 'c').replace('T', 'a').toUpperCase();
			sb.append(line);
			if (i < lines.length - 1)
				sb.append('\n');
		}
		return sb.toString();
	}

	private static List<PairRecord> zip(List<Record> left, List<Record> right) {
		List<PairRecord> pairs = new ArrayList<>();
		for (int i = 0; i < left.size(); i++)
			pairs.add(new PairRecord(left.get(i).key, left.get(i).value, right.get(i).value));
		return pairs;
	}

	private static List<String> leftValues(List<PairRecord> pairs) {
		List<String> l = new ArrayList<>();
		for (PairRecord p : pairs)
			l.add(p.left);
		return l;
	}

	private static List<String> rightValues(List<PairRecord> pairs) {
		List<String> r = new ArrayList<>();
		for (PairRecord p : pairs)
			r.add(p.right);
		return r;
	}

	private void configure(Path left, Path right, Class<? extends SingleEndSequenceInputFormat> format) throws IOException {
		PairedEndSequenceInputFormat.setLeftInputPath(conf, left, format);
		PairedEndSequenceInputFormat.setRightInputPath(conf, right, format);
	}

	private void assertIOException(String messagePart, Runnable0 action) {
		try {
			action.run();
			fail("expected IOException containing '" + messagePart + "'");
		} catch (IOException e) {
			assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
		} catch (Exception e) {
			throw new AssertionError("unexpected exception", e);
		}
	}

	private interface Runnable0 {
		void run() throws Exception;
	}

	// ---------------------------------------------------------------------
	// Configuration
	// ---------------------------------------------------------------------

	@Test
	public void inputPathsAndFormatsAreStoredInTheConfiguration() throws Exception {
		Path left = TestSupport.write(tmp.getRoot(), "l.fq", "@a\nA\n+\nI\n");
		Path right = TestSupport.write(tmp.getRoot(), "r.fq", "@a\nT\n+\nI\n");

		configure(left, right, FastQInputFormat.class);
		Job job = Job.getInstance(conf);

		assertArrayEquals(new Path[] { left, right }, PairedEndSequenceInputFormat.getInputPaths(job));
		assertArrayEquals(new String[] { FastQInputFormat.class.getName(), FastQInputFormat.class.getName() }, PairedEndSequenceInputFormat.getInputFormats(job));
	}

	@Test
	public void jobOverloadsStoreTheSameAsConfigurationOverloads() throws Exception {
		Path left = TestSupport.write(tmp.getRoot(), "l.fa", ">a\nA\n");
		Path right = TestSupport.write(tmp.getRoot(), "r.fa", ">a\nT\n");
		Job job = Job.getInstance(conf);

		PairedEndSequenceInputFormat.setLeftInputPath(job, left, FastAInputFormat.class);
		PairedEndSequenceInputFormat.setRightInputPath(job, right, FastAInputFormat.class);

		assertArrayEquals(new Path[] { left, right }, PairedEndSequenceInputFormat.getInputPaths(job));
		assertArrayEquals(new String[] { FastAInputFormat.class.getName(), FastAInputFormat.class.getName() }, PairedEndSequenceInputFormat.getInputFormats(job));
	}

	@Test
	public void pathsWithSpecialCharactersSurviveTheRoundTrip() throws Exception {
		File dir = tmp.newFolder("a,b c");
		Path left = TestSupport.write(dir, "l,1.fq", "@a\nA\n+\nI\n");
		Path right = TestSupport.write(dir, "r,1.fq", "@a\nT\n+\nI\n");

		configure(left, right, FastQInputFormat.class);

		assertArrayEquals(new Path[] { left, right }, PairedEndSequenceInputFormat.getInputPaths(Job.getInstance(conf)));
	}

	@Test
	public void inputPathMustBeAnExistingNonEmptyFile() throws Exception {
		final Path ok = TestSupport.write(tmp.getRoot(), "ok.fq", "@a\nA\n+\nI\n");
		final Path empty = TestSupport.write(tmp.getRoot(), "empty.fq", "");
		final Path missing = new Path(new File(tmp.getRoot(), "missing.fq").toURI());
		final File dir = tmp.newFolder("dir");
		TestSupport.write(dir, "a.fq", "@a\nA\n+\nI\n");
		TestSupport.write(dir, "b.fq", "@a\nA\n+\nI\n");
		final File subdir = tmp.newFolder("onlysub");
		new File(subdir, "sub").mkdir();

		assertIOException("is empty", () -> PairedEndSequenceInputFormat.setLeftInputPath(conf, empty, FastQInputFormat.class));
		assertIOException("does not exist", () -> PairedEndSequenceInputFormat.setRightInputPath(conf, missing, FastQInputFormat.class));
		assertIOException("matches multiple files", () -> PairedEndSequenceInputFormat.setLeftInputPath(conf, new Path(dir.toURI()), FastQInputFormat.class));
		assertIOException("must be a sequence file", () -> PairedEndSequenceInputFormat.setLeftInputPath(conf, new Path(subdir.toURI()), FastQInputFormat.class));
		PairedEndSequenceInputFormat.setLeftInputPath(conf, ok, FastQInputFormat.class);
	}

	@Test
	public void aDirectoryWithASingleFileIsAccepted() throws Exception {
		File dir = tmp.newFolder("single");
		TestSupport.write(dir, "a.fq", "@a\nA\n+\nI\n");

		PairedEndSequenceInputFormat.setLeftInputPath(conf, new Path(dir.toURI()), FastQInputFormat.class);
	}

	@Test
	public void splittabilityFollowsTheCodec() throws Exception {
		Job job = Job.getInstance(conf);
		PairedEndSequenceInputFormat format = new PairedEndSequenceInputFormat();

		assertTrue(format.isSplitable(job, new Path("file:///a.fq")));
		assertTrue(format.isSplitable(job, new Path("file:///a.fq.bz2")));
		assertTrue(!format.isSplitable(job, new Path("file:///a.fq.gz")));
	}

	@Test
	public void numberOfSplitsMatchesTheSplitsGeneratedByHadoop() throws Exception {
		String leftText = TestSupport.fastq(200, 1, true);
		long length = leftText.getBytes(StandardCharsets.UTF_8).length;
		Path left = TestSupport.write(tmp.getRoot(), "l.fq", leftText);
		configure(left, TestSupport.write(tmp.getRoot(), "r.fq", mateOfFastq(leftText)), FastQInputFormat.class);

		for (long splitSize : new long[] { 1000, 1024, 4096, 5000, length / 2, length - 1, length, length + 1, 10 * length }) {
			Job job = Job.getInstance(conf);
			FileInputFormat.setMaxInputSplitSize(job, splitSize);
			int generated = new PairedEndSequenceInputFormat().getSplits(job).size();
			assertEquals("split size " + splitSize, generated, PairedEndSequenceInputFormat.getNumberOfSplits(left, length, true, splitSize));
		}
		assertEquals(1, PairedEndSequenceInputFormat.getNumberOfSplits(left, length, false, 1000));
		assertEquals(0, PairedEndSequenceInputFormat.getNumberOfSplits(left, 0, true, 1000));
	}

	// ---------------------------------------------------------------------
	// getSplits() validation
	// ---------------------------------------------------------------------

	@Test
	public void leftAndRightFormatsMustMatch() throws Exception {
		Path left = TestSupport.write(tmp.getRoot(), "l.fq", "@a\nA\n+\nI\n");
		Path right = TestSupport.write(tmp.getRoot(), "r.fa", ">a\nA\n");
		PairedEndSequenceInputFormat.setLeftInputPath(conf, left, FastQInputFormat.class);
		PairedEndSequenceInputFormat.setRightInputPath(conf, right, FastAInputFormat.class);

		assertIOException("Input formats do not match", () -> new PairedEndSequenceInputFormat().getSplits(Job.getInstance(conf)));
	}

	@Test
	public void numberOfSplitsOfBothFilesMustMatch() throws Exception {
		Path left = TestSupport.write(tmp.getRoot(), "l.fq", TestSupport.fastq(100, 1, true));
		Path right = TestSupport.write(tmp.getRoot(), "r.fq", TestSupport.fastq(10, 1, true));
		configure(left, right, FastQInputFormat.class);

		assertIOException("Number of file splits does not match", () -> TestSupport.readAllPairedSplits(conf, 1000));
	}

	@Test
	public void unknownInputFormatClassIsReportedAsIOException() throws Exception {
		Path left = TestSupport.write(tmp.getRoot(), "l.fq", "@a\nA\n+\nI\n");
		Path right = TestSupport.write(tmp.getRoot(), "r.fq", "@a\nT\n+\nI\n");
		configure(left, right, FastQInputFormat.class);
		conf.set(PairedEndSequenceInputFormat.LEFT_INPUT_FORMAT, "does.not.Exist");
		conf.set(PairedEndSequenceInputFormat.RIGHT_INPUT_FORMAT, "does.not.Exist");

		assertIOException("does.not.Exist", () -> new PairedEndSequenceInputFormat().getSplits(Job.getInstance(conf)));
	}

	@Ignore("BUG: an unconfigured job fails with IllegalArgumentException (empty Path) instead of the IOException of the explicit checks")
	@Test
	public void unconfiguredJobIsReportedAsIOException() throws Exception {
		try {
			new PairedEndSequenceInputFormat().getSplits(Job.getInstance(conf));
			fail("expected IOException");
		} catch (IOException expected) {
			// ok
		}
	}

	// ---------------------------------------------------------------------
	// Reading
	// ---------------------------------------------------------------------

	@Test
	public void fastqPairsAreReadInLockstepForAnySplitSize() throws Exception {
		String leftText = TestSupport.fastq(300, 1, true);
		String rightText = mateOfFastq(leftText);
		long length = leftText.getBytes(StandardCharsets.UTF_8).length;
		configure(TestSupport.write(tmp.getRoot(), "l.fq", leftText), TestSupport.write(tmp.getRoot(), "r.fq", rightText), FastQInputFormat.class);
		List<PairRecord> expected = zip(TestSupport.expectedFastq(leftText, true), TestSupport.expectedFastq(rightText, true));

		for (long splitSize : new long[] { 1000, 4096, 5000, length / 2, length, 2 * length }) {
			List<PairRecord> actual = TestSupport.readAllPairedSplits(conf, splitSize);
			assertEquals("split size " + splitSize, leftValues(expected), leftValues(actual));
			assertEquals("split size " + splitSize, rightValues(expected), rightValues(actual));
		}
	}

	@Test
	public void fastaPairsAreReadInLockstepForAnySplitSize() throws Exception {
		String leftText = TestSupport.fasta(300, 2, true);
		String rightText = mateOfFasta(leftText);
		long length = leftText.getBytes(StandardCharsets.UTF_8).length;
		configure(TestSupport.write(tmp.getRoot(), "l.fa", leftText), TestSupport.write(tmp.getRoot(), "r.fa", rightText), FastAInputFormat.class);
		List<PairRecord> expected = zip(TestSupport.expectedFasta(leftText, true), TestSupport.expectedFasta(rightText, true));

		for (long splitSize : new long[] { 1000, 4096, length / 2, length, 2 * length }) {
			List<PairRecord> actual = TestSupport.readAllPairedSplits(conf, splitSize);
			assertEquals("split size " + splitSize, leftValues(expected), leftValues(actual));
			assertEquals("split size " + splitSize, rightValues(expected), rightValues(actual));
		}
	}

	@Test
	public void pairsAreReadInLockstepWithSplitsShorterThanARecord() throws Exception {
		String leftText = TestSupport.fastq(120, 7, true);
		String rightText = mateOfFastq(leftText);
		configure(TestSupport.write(tmp.getRoot(), "l.fq", leftText), TestSupport.write(tmp.getRoot(), "r.fq", rightText), FastQInputFormat.class);
		List<PairRecord> expected = zip(TestSupport.expectedFastq(leftText, true), TestSupport.expectedFastq(rightText, true));

		for (long splitSize : new long[] { 7, 20, 50, 77 }) {
			List<PairRecord> actual = TestSupport.readAllPairedSplits(conf, splitSize);
			assertEquals("split size " + splitSize, leftValues(expected), leftValues(actual));
			assertEquals("split size " + splitSize, rightValues(expected), rightValues(actual));
		}
	}

	@Test
	public void keyIsTheOffsetOfTheRecordInBothFiles() throws Exception {
		String leftText = TestSupport.fastq(50, 3, true);
		String rightText = mateOfFastq(leftText);
		configure(TestSupport.write(tmp.getRoot(), "l.fq", leftText), TestSupport.write(tmp.getRoot(), "r.fq", rightText), FastQInputFormat.class);
		List<Record> expected = TestSupport.expectedFastq(leftText, true);

		List<PairRecord> actual = TestSupport.readAllPairedSplits(conf, 10_000_000);

		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < actual.size(); i++)
			assertEquals(expected.get(i).key, actual.get(i).key);
	}

	@Test
	public void pairedSplitsHaveOneChildSplitPerFileAndTheCommonHostsAsLocations() throws Exception {
		String leftText = TestSupport.fastq(300, 4, true);
		Path left = TestSupport.write(tmp.getRoot(), "l.fq", leftText);
		Path right = TestSupport.write(tmp.getRoot(), "r.fq", mateOfFastq(leftText));
		configure(left, right, FastQInputFormat.class);
		Job job = Job.getInstance(conf);
		FileInputFormat.setMaxInputSplitSize(job, 3000);

		List<InputSplit> splits = new PairedEndSequenceInputFormat().getSplits(job);

		assertTrue(splits.size() > 1);
		for (InputSplit s : splits) {
			PairedEndInputSplit split = (PairedEndInputSplit) s;
			FileSplit l = split.get(0), r = split.get(1);
			assertEquals(left, l.getPath());
			assertEquals(right, r.getPath());
			assertEquals(l.getStart(), r.getStart());
			assertEquals(l.getLength(), r.getLength());
			assertEquals(l.getLength() + r.getLength(), split.getLength());
			assertArrayEquals(new String[] { "localhost" }, split.getLocations());
		}
	}

	@Test
	public void gzipPairsAreReadInASingleSplit() throws Exception {
		String leftText = TestSupport.fastq(200, 5, true);
		String rightText = mateOfFastq(leftText);
		Path left = TestSupport.write(tmp.getRoot(), "l.fq.gz", TestSupport.compress(GzipCodec.class, conf, leftText.getBytes(StandardCharsets.UTF_8)));
		Path right = TestSupport.write(tmp.getRoot(), "r.fq.gz", TestSupport.compress(GzipCodec.class, conf, rightText.getBytes(StandardCharsets.UTF_8)));
		configure(left, right, FastQInputFormat.class);
		List<PairRecord> expected = zip(TestSupport.expectedFastq(leftText, true), TestSupport.expectedFastq(rightText, true));

		Job job = Job.getInstance(conf);
		FileInputFormat.setMaxInputSplitSize(job, 100);
		assertEquals(1, new PairedEndSequenceInputFormat().getSplits(job).size());
		assertEquals(expected, TestSupport.readAllPairedSplits(conf, 100));
	}

	@Test
	public void bzip2PairsAreReadInASingleSplit() throws Exception {
		Assume.assumeFalse("Hadoop 2.x: bzip2 readers open at the same time interfere with each other", VersionInfo.getVersion().startsWith("2."));
		String leftText = TestSupport.fastq(30000, 6, true);
		String rightText = mateOfFastq(leftText);
		Path left = TestSupport.write(tmp.getRoot(), "l.fq.bz2", TestSupport.compress(BZip2Codec.class, conf, leftText.getBytes(StandardCharsets.UTF_8)));
		Path right = TestSupport.write(tmp.getRoot(), "r.fq.bz2", TestSupport.compress(BZip2Codec.class, conf, rightText.getBytes(StandardCharsets.UTF_8)));
		configure(left, right, FastQInputFormat.class);
		List<PairRecord> expected = zip(TestSupport.expectedFastq(leftText, true), TestSupport.expectedFastq(rightText, true));

		List<PairRecord> actual = TestSupport.readAllPairedSplits(conf, 100_000_000);

		assertEquals(leftValues(expected), leftValues(actual));
		assertEquals(rightValues(expected), rightValues(actual));
	}

	/**
	 * The splits of each file are computed independently in compressed bytes. Both files compress to
	 * different sizes, so the i-th split of each one does not contain the same records.
	 */
	@Ignore("BUG: splits of a splittable compressed input (bzip2) are computed per file in compressed bytes, so both files get a different number of splits or splits with different records")
	@Test
	public void bzip2PairsAreReadInSeveralSplits() throws Exception {
		String leftText = TestSupport.fastq(30000, 6, true);
		String rightText = lowEntropyMateOfFastq(leftText);
		Path left = TestSupport.write(tmp.getRoot(), "l.fq.bz2", TestSupport.compress(BZip2Codec.class, conf, leftText.getBytes(StandardCharsets.UTF_8)));
		Path right = TestSupport.write(tmp.getRoot(), "r.fq.bz2", TestSupport.compress(BZip2Codec.class, conf, rightText.getBytes(StandardCharsets.UTF_8)));
		configure(left, right, FastQInputFormat.class);
		List<PairRecord> expected = zip(TestSupport.expectedFastq(leftText, true), TestSupport.expectedFastq(rightText, true));

		Job job = Job.getInstance(conf);
		FileInputFormat.setMaxInputSplitSize(job, 250_000);
		assertTrue(new PairedEndSequenceInputFormat().getSplits(job).size() > 1);

		List<PairRecord> actual = TestSupport.readAllPairedSplits(conf, 250_000);
		assertEquals(leftValues(expected), leftValues(actual));
		assertEquals(rightValues(expected), rightValues(actual));
	}

	// ---------------------------------------------------------------------
	// Mismatched mates
	// ---------------------------------------------------------------------

	private void assertReadFails(String leftText, String rightText, String messagePart) throws Exception {
		configure(TestSupport.write(tmp.getRoot(), "l.fq", leftText), TestSupport.write(tmp.getRoot(), "r.fq", rightText), FastQInputFormat.class);
		assertIOException(messagePart, () -> TestSupport.readAllPairedSplits(conf, 10_000_000));
	}

	@Test
	public void rightFileWithFewerRecordsIsReported() throws Exception {
		assertReadFails("@a\nA\n+\nI\n@b\nC\n+\nJ\n", "@a\nT\n+\nI\n", "Unexpected end of split for right record reader");
	}

	@Test
	public void leftFileWithFewerRecordsIsReported() throws Exception {
		assertReadFails("@a\nA\n+\nI\n", "@a\nT\n+\nI\n@b\nG\n+\nJ\n", "Unexpected end of split for left record reader");
	}

	@Test
	public void recordsAtDifferentOffsetsAreReported() throws Exception {
		// Same trimmed record, but the first record is longer in the left file, so the second one is at a different offset
		assertReadFails("@a x\nA\n+\nI\n@b\nC\n+\nJ\n", "@a\nT\n+\nI\n@b\nG\n+\nJ\n", "Unexpected different keys");
	}

	@Test
	public void recordsWithDifferentLengthAtTheSameOffsetAreReported() throws Exception {
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setTrimSequenceName(conf, false);
		assertReadFails("@a x\nAC\n+\nII\n", "@a\nACGT\n+\nIIII\n", "Unexpected different");
	}

	@Test
	public void pairedReadsAreDecodedAsStrings() throws Exception {
		PairText pair = new PairText(new org.apache.hadoop.io.Text("@a\nA\n+\nI\n"), new org.apache.hadoop.io.Text("@a\nT\n+\nI\n"));

		assertEquals("@a\nA\n+\nI\n", PairedEndSequenceRecordReader.getLeftRead(pair));
		assertEquals("@a\nT\n+\nI\n", PairedEndSequenceRecordReader.getRightRead(pair));
	}
}
