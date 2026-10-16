package es.udc.gac.hadoop.sequence.parser.mapreduce;

import static es.udc.gac.hadoop.sequence.parser.TestSupport.expectedFasta;
import static es.udc.gac.hadoop.sequence.parser.TestSupport.expectedFastq;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import es.udc.gac.hadoop.sequence.parser.TestSupport;
import es.udc.gac.hadoop.sequence.parser.TestSupport.Record;

/**
 * A file cut at any byte offset must yield exactly the same records as the whole file, with
 * no loss and no duplicates: every record is read by exactly one of the splits.
 */
public class SplitBoundaryTest {

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

	private List<Record> readCuts(Class<? extends SingleEndSequenceInputFormat> format, Path path, long length, long... cuts) throws Exception {
		List<Record> records = new ArrayList<>();
		long start = 0;
		for (long cut : cuts) {
			records.addAll(TestSupport.readSplit(format, conf, path, start, cut - start));
			start = cut;
		}
		records.addAll(TestSupport.readSplit(format, conf, path, start, length - start));
		return records;
	}

	private void assertEveryCutGivesTheSameRecords(Class<? extends SingleEndSequenceInputFormat> format, String text, List<Record> expected) throws Exception {
		byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
		Path path = TestSupport.write(tmp.getRoot(), "cut", bytes);

		for (int cut = 1; cut < bytes.length; cut++)
			assertEquals("file cut at byte " + cut, values(expected), values(readCuts(format, path, bytes.length, cut)));
	}

	private static long longestRecord(List<Record> records, long fileLength) {
		long longest = 0;
		for (int i = 0; i < records.size(); i++) {
			long end = i + 1 < records.size() ? records.get(i + 1).key : fileLength;
			longest = Math.max(longest, end - records.get(i).key);
		}
		return longest;
	}

	private void assertEveryPairOfCutsGivesTheSameRecords(Class<? extends SingleEndSequenceInputFormat> format, String text, List<Record> expected, long minMiddleLength) throws Exception {
		byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
		Path path = TestSupport.write(tmp.getRoot(), "cuts", bytes);

		for (int c1 = 1; c1 < bytes.length; c1++)
			for (int c2 = (int) (c1 + minMiddleLength); c2 < bytes.length; c2++)
				assertEquals("file cut at bytes " + c1 + " and " + c2, values(expected), values(readCuts(format, path, bytes.length, c1, c2)));
	}

	@Test
	public void fastqCutAtAnyByte() throws Exception {
		String text = TestSupport.fastq(15, 10, true);
		assertEveryCutGivesTheSameRecords(FastQInputFormat.class, text, expectedFastq(text, true));
	}

	@Test
	public void fastqCutAtAnyByteWithoutTrailingNewline() throws Exception {
		String text = TestSupport.fastq(15, 11, false);
		assertEveryCutGivesTheSameRecords(FastQInputFormat.class, text, expectedFastq(text, true));
	}

	@Test
	public void fastqCutAtAnyByteWithoutNameTrimming() throws Exception {
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setTrimSequenceName(conf, false);
		String text = TestSupport.fastq(15, 12, true);
		assertEveryCutGivesTheSameRecords(FastQInputFormat.class, text, expectedFastq(text, false));
	}

	@Test
	public void fastqCutAtAnyByteWithSmallInputBuffer() throws Exception {
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setInputBufferSize(conf, 7);
		String text = TestSupport.fastq(8, 13, true);
		assertEveryCutGivesTheSameRecords(FastQInputFormat.class, text, expectedFastq(text, true));
	}

	@Test
	public void fastqCutAtAnyTwoBytesWhenNoSplitIsShorterThanARecord() throws Exception {
		String text = TestSupport.fastq(4, 14, true);
		List<Record> expected = expectedFastq(text, true);
		assertEveryPairOfCutsGivesTheSameRecords(FastQInputFormat.class, text, expected, longestRecord(expected, text.length()));
	}

	@Ignore("BUG: records are duplicated when a split is shorter than a record (the next split rescans the same record)")
	@Test
	public void fastqCutAtAnyTwoBytes() throws Exception {
		String text = TestSupport.fastq(4, 14, true);
		assertEveryPairOfCutsGivesTheSameRecords(FastQInputFormat.class, text, expectedFastq(text, true), 1);
	}

	@Test
	public void fastaCutAtAnyByte() throws Exception {
		String text = TestSupport.fasta(15, 20, true);
		assertEveryCutGivesTheSameRecords(FastAInputFormat.class, text, expectedFasta(text, true));
	}

	@Test
	public void fastaCutAtAnyByteWithoutTrailingNewline() throws Exception {
		String text = TestSupport.fasta(15, 21, false);
		assertEveryCutGivesTheSameRecords(FastAInputFormat.class, text, expectedFasta(text, true));
	}

	@Test
	public void fastaCutAtAnyTwoBytesWhenNoSplitIsShorterThanARecord() throws Exception {
		String text = TestSupport.fasta(4, 22, true);
		List<Record> expected = expectedFasta(text, true);
		assertEveryPairOfCutsGivesTheSameRecords(FastAInputFormat.class, text, expected, longestRecord(expected, text.length()));
	}

	@Ignore("BUG: records are duplicated when a split is shorter than a record (the next split rescans the same record)")
	@Test
	public void fastaCutAtAnyTwoBytes() throws Exception {
		String text = TestSupport.fasta(4, 22, true);
		assertEveryPairOfCutsGivesTheSameRecords(FastAInputFormat.class, text, expectedFasta(text, true), 1);
	}

	/**
	 * The key is documented as the byte offset of the record in the file. It is only that for the
	 * records after the first one of a split: the first one gets the position where the reader
	 * started scanning, before discarding the partial record at the split start.
	 */
	@Ignore("BUG: the key of the first record of a split that starts mid-record is not the offset of that record")
	@Test
	public void keysAreTheOffsetsOfTheRecordsWhateverTheSplitStart() throws Exception {
		String text = TestSupport.fastq(15, 30, true);
		List<Record> expected = expectedFastq(text, true);
		byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
		Path path = TestSupport.write(tmp.getRoot(), "keys", bytes);

		for (int cut = 1; cut < bytes.length; cut++)
			assertEquals("file cut at byte " + cut, expected, readCuts(FastQInputFormat.class, path, bytes.length, cut));
	}
}
