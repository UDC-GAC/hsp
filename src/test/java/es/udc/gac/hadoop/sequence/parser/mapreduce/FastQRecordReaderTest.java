package es.udc.gac.hadoop.sequence.parser.mapreduce;

import static es.udc.gac.hadoop.sequence.parser.TestSupport.expectedFastq;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import es.udc.gac.hadoop.sequence.parser.TestSupport;
import es.udc.gac.hadoop.sequence.parser.TestSupport.Record;

/** Behaviour of the FASTQ reader when a file is read as a single split. */
public class FastQRecordReaderTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	private Configuration conf;

	@Before
	public void setUp() {
		conf = TestSupport.newConf();
	}

	private List<Record> read(String content) throws Exception {
		byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
		Path path = TestSupport.write(tmp.getRoot(), "reads.fq", bytes);
		return TestSupport.readWhole(FastQInputFormat.class, conf, path, bytes.length);
	}

	@Test
	public void singleRecordIsEmittedWithNameTrimmedAndPlusLineWithoutComment() throws Exception {
		List<Record> records = read("@r1 some comment\nACGT\n+r1 some comment\nIIII\n");

		assertEquals(Arrays.asList(new Record(0, "@r1\nACGT\n+\nIIII\n")), records);
	}

	@Test
	public void lastLineWithOnlyAnAtSignAfterAHeaderIsAnIncompleteRecord() throws Exception {
		// the reader looks one line ahead of a header and that line is shorter than the position where the name is trimmed
		List<Record> records = read("@a\nA\n+\nI\n@b\n@");

		assertEquals(Arrays.asList(new Record(0, "@a\nA\n+\nI\n")), records);
	}

	@Test
	public void nameIsKeptByDefault() throws Exception {
		conf = new Configuration(false);

		List<Record> records = read("@r1 some comment\nACGT\n+r1 some comment\nIIII\n");

		assertEquals(Arrays.asList(new Record(0, "@r1 some comment\nACGT\n+\nIIII\n")), records);
	}

	@Test
	public void nameIsKeptWhenTrimmingIsDisabled() throws Exception {
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setTrimSequenceName(conf, false);

		List<Record> records = read("@r1 some comment\nACGT\n+r1\nIIII\n");

		assertEquals(Arrays.asList(new Record(0, "@r1 some comment\nACGT\n+\nIIII\n")), records);
	}

	@Test
	public void keysAreTheByteOffsetsOfEachRecord() throws Exception {
		String r1 = "@r1\nACGT\n+\nIIII\n";
		String r2 = "@r2 x\nAC\n+\nII\n";
		List<Record> records = read(r1 + r2 + "@r3\nA\n+\nI\n");

		assertEquals(3, records.size());
		assertEquals(0, records.get(0).key);
		assertEquals(r1.length(), records.get(1).key);
		assertEquals(r1.length() + r2.length(), records.get(2).key);
	}

	@Test
	public void manyRecordsMatchTheExpectedOutput() throws Exception {
		String text = TestSupport.fastq(500, 1, true);

		assertEquals(expectedFastq(text, true), read(text));
	}

	@Test
	public void manyRecordsMatchTheExpectedOutputWithoutTrimming() throws Exception {
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setTrimSequenceName(conf, false);
		String text = TestSupport.fastq(500, 2, true);

		assertEquals(expectedFastq(text, false), read(text));
	}

	@Test
	public void lastRecordWithoutTrailingNewlineIsEmittedWithoutIt() throws Exception {
		String text = TestSupport.fastq(20, 3, false);
		List<Record> records = read(text);

		assertEquals(expectedFastq(text, true), records);
		assertTrue(!records.get(19).value.endsWith("\n"));
	}

	@Test
	public void qualityLinesStartingWithAtSignAreNotMistakenForHeaders() throws Exception {
		String text = "@r1\nACGT\n+\n@III\n@r2\nACGT\n+\n@@@@\n@r3\nAC\n+\n@@\n";

		assertEquals(Arrays.asList(
				new Record(0, "@r1\nACGT\n+\n@III\n"),
				new Record(16, "@r2\nACGT\n+\n@@@@\n"),
				new Record(32, "@r3\nAC\n+\n@@\n")), read(text));
	}

	@Test
	public void emptyFileHasNoRecords() throws Exception {
		assertTrue(read("").isEmpty());
	}

	@Test
	public void incompleteLastRecordIsDropped() throws Exception {
		List<Record> records = read("@r1\nACGT\n+\nIIII\n@r2\nAC\n");

		assertEquals(Arrays.asList(new Record(0, "@r1\nACGT\n+\nIIII\n")), records);
	}

	@Test
	public void sameRecordsForAnyInputBufferSize() throws Exception {
		String text = TestSupport.fastq(60, 4, true);
		List<Record> expected = expectedFastq(text, true);

		for (int bufferSize : new int[] { 1, 2, 3, 7, 16, 61, 100, 4096 }) {
			es.udc.gac.hadoop.sequence.parser.util.Configuration.setInputBufferSize(conf, bufferSize);
			assertEquals("buffer size " + bufferSize, expected, read(text));
		}
	}

	@Test
	public void progressGoesFromZeroToOne() throws Exception {
		String text = TestSupport.fastq(50, 5, true);
		byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
		Path path = TestSupport.write(tmp.getRoot(), "progress.fq", bytes);
		FileSplit split = new FileSplit(path, 0, bytes.length, null);
		org.apache.hadoop.mapreduce.TaskAttemptContext ctx = TestSupport.context(conf);
		org.apache.hadoop.mapreduce.RecordReader<LongWritable, Text> reader = new FastQInputFormat().createRecordReader(split, ctx);

		try {
			reader.initialize(split, ctx);
			assertEquals(0.0f, reader.getProgress(), 0.0f);
			float previous = 0;
			while (reader.nextKeyValue()) {
				float progress = reader.getProgress();
				assertTrue(progress >= previous && progress <= 1.0f);
				previous = progress;
			}
			assertEquals(1.0f, reader.getProgress(), 0.0f);
		} finally {
			reader.close();
		}
	}

	@Test
	public void valueIsReusedBetweenRecordsButKeyAndValueAreCurrent() throws Exception {
		byte[] bytes = "@r1\nA\n+\nI\n@r2\nC\n+\nJ\n".getBytes(StandardCharsets.UTF_8);
		Path path = TestSupport.write(tmp.getRoot(), "reuse.fq", bytes);
		FileSplit split = new FileSplit(path, 0, bytes.length, null);
		org.apache.hadoop.mapreduce.TaskAttemptContext ctx = TestSupport.context(conf);
		org.apache.hadoop.mapreduce.RecordReader<LongWritable, Text> reader = new FastQInputFormat().createRecordReader(split, ctx);

		try {
			reader.initialize(split, ctx);
			assertTrue(reader.nextKeyValue());
			Text first = reader.getCurrentValue();
			assertEquals("@r1\nA\n+\nI\n", first.toString());
			assertTrue(reader.nextKeyValue());
			assertEquals("@r2\nC\n+\nJ\n", reader.getCurrentValue().toString());
			assertEquals(10, reader.getCurrentKey().get());
		} finally {
			reader.close();
		}
	}
}
