package es.udc.gac.hadoop.sequence.parser.mapreduce;

import static es.udc.gac.hadoop.sequence.parser.TestSupport.expectedFasta;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import es.udc.gac.hadoop.sequence.parser.TestSupport;
import es.udc.gac.hadoop.sequence.parser.TestSupport.Record;

/** Behaviour of the FASTA reader when a file is read as a single split. */
public class FastARecordReaderTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	private Configuration conf;

	@Before
	public void setUp() {
		conf = TestSupport.newConf();
	}

	private List<Record> read(String content) throws Exception {
		byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
		Path path = TestSupport.write(tmp.getRoot(), "reads.fa", bytes);
		return TestSupport.readWhole(FastAInputFormat.class, conf, path, bytes.length);
	}

	@Test
	public void singleRecordHasNameTrimmed() throws Exception {
		assertEquals(Arrays.asList(new Record(0, ">seq1\nACGT\n")), read(">seq1 some description\nACGT\n"));
	}

	@Test
	public void nameIsKeptWhenTrimmingIsDisabled() throws Exception {
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setTrimSequenceName(conf, false);

		assertEquals(Arrays.asList(new Record(0, ">seq1 some description\nACGT\n")), read(">seq1 some description\nACGT\n"));
	}

	@Test
	public void multiLineSequencesStayInTheSameRecord() throws Exception {
		String text = ">s1 d\nACGT\nGGCC\nTT\n>s2\nAAAA\n";

		assertEquals(Arrays.asList(
				new Record(0, ">s1\nACGT\nGGCC\nTT\n"),
				new Record(19, ">s2\nAAAA\n")), read(text));
	}

	@Test
	public void keysAreTheByteOffsetsOfEachRecord() throws Exception {
		String r1 = ">s1\nACGT\n";
		String r2 = ">s2 x\nAC\nGT\n";
		List<Record> records = read(r1 + r2 + ">s3\nA\n");

		assertEquals(3, records.size());
		assertEquals(0, records.get(0).key);
		assertEquals(r1.length(), records.get(1).key);
		assertEquals(r1.length() + r2.length(), records.get(2).key);
	}

	@Test
	public void manyRecordsMatchTheExpectedOutput() throws Exception {
		String text = TestSupport.fasta(300, 1, true);

		assertEquals(expectedFasta(text, true), read(text));
	}

	@Test
	public void manyRecordsMatchTheExpectedOutputWithoutTrimming() throws Exception {
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setTrimSequenceName(conf, false);
		String text = TestSupport.fasta(300, 2, true);

		assertEquals(expectedFasta(text, false), read(text));
	}

	@Test
	public void lastRecordWithoutTrailingNewlineIsEmittedWithoutIt() throws Exception {
		String text = TestSupport.fasta(20, 3, false);
		List<Record> records = read(text);

		assertEquals(expectedFasta(text, true), records);
		assertTrue(!records.get(19).value.endsWith("\n"));
	}

	@Test
	public void recordWithHeaderOnlyIsEmitted() throws Exception {
		assertEquals(Arrays.asList(new Record(0, ">s1\n"), new Record(4, ">s2\nAC\n")), read(">s1\n>s2\nAC\n"));
	}

	@Test
	public void emptyFileHasNoRecords() throws Exception {
		assertTrue(read("").isEmpty());
	}

	/** The header of the next record may be split between two buffer refills at any point. */
	@Test
	public void sameRecordsForAnyInputBufferSize() throws Exception {
		String text = TestSupport.fasta(60, 4, true);
		List<Record> expected = expectedFasta(text, true);

		for (int bufferSize : new int[] { 1, 2, 3, 5, 7, 16, 100, 128, 200, 500, 4096 }) {
			es.udc.gac.hadoop.sequence.parser.util.Configuration.setInputBufferSize(conf, bufferSize);
			assertEquals("buffer size " + bufferSize, expected, read(text));
		}
	}

	@Test
	public void sameRecordsForEveryBufferSizeUpToTheFileSize() throws Exception {
		String text = TestSupport.fasta(8, 5, false);
		List<Record> expected = expectedFasta(text, true);

		for (int bufferSize = 1; bufferSize <= text.length() + 1; bufferSize++) {
			es.udc.gac.hadoop.sequence.parser.util.Configuration.setInputBufferSize(conf, bufferSize);
			assertEquals("buffer size " + bufferSize, expected, read(text));
		}
	}

	@Test
	public void sameRecordsWhenTheWholeFileFitsInTheInputBuffer() throws Exception {
		String text = TestSupport.fasta(60, 4, true);
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setInputBufferSize(conf, 1 << 16);

		assertEquals(expectedFasta(text, true), read(text));
	}
}
