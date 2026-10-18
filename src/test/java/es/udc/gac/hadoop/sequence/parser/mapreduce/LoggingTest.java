package es.udc.gac.hadoop.sequence.parser.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import es.udc.gac.hadoop.sequence.parser.TestSupport;
import es.udc.gac.hadoop.sequence.parser.TestSupport.Record;

/**
 * What the readers log. HSP only depends on the SLF4J API, so the events are captured from the backend that
 * Hadoop provides (Log4j 1.x API, as the tests run with it). The most verbose level is enabled on purpose:
 * logging must not depend on the number of records.
 */
public class LoggingTest {

	private static final String ROOT = "es.udc.gac.hadoop.sequence.parser";

	private static class Capture extends AppenderSkeleton {
		final List<LoggingEvent> events = new ArrayList<>();

		@Override
		protected void append(LoggingEvent event) {
			events.add(event);
		}

		@Override
		public void close() {
		}

		@Override
		public boolean requiresLayout() {
			return false;
		}

		List<String> messages(Level level) {
			List<String> messages = new ArrayList<>();
			for (LoggingEvent event : events) {
				if (event.getLevel().equals(level))
					messages.add(event.getRenderedMessage());
			}
			return messages;
		}
	}

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	private Configuration conf;
	private Capture capture;
	private Level previousLevel;
	private boolean previousAdditivity;

	@Before
	public void setUp() {
		conf = TestSupport.newConf();
		capture = new Capture();
		Logger logger = Logger.getLogger(ROOT);
		previousLevel = logger.getLevel();
		previousAdditivity = logger.getAdditivity();
		logger.setAdditivity(false);
		logger.setLevel(Level.ALL);
		logger.addAppender(capture);
	}

	@After
	public void tearDown() {
		Logger logger = Logger.getLogger(ROOT);
		logger.removeAppender(capture);
		logger.setLevel(previousLevel);
		logger.setAdditivity(previousAdditivity);
	}

	private int eventsReading(Class<? extends SingleEndSequenceInputFormat> format, String text, boolean gzip) throws Exception {
		byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
		String name = format == FastQInputFormat.class ? "reads.fq" : "reads.fa";
		Path path;
		if (gzip) {
			path = TestSupport.write(tmp.newFolder(), name + ".gz", TestSupport.compress(GzipCodec.class, conf, bytes));
		} else {
			path = TestSupport.write(tmp.newFolder(), name, bytes);
		}
		capture.events.clear();
		TestSupport.readWhole(format, conf, path, gzip ? Long.MAX_VALUE / 2 : bytes.length);
		return capture.events.size();
	}

	@Test
	public void nothingIsLoggedPerRecord() throws Exception {
		for (boolean gzip : new boolean[] { false, true }) {
			int fastqFew = eventsReading(FastQInputFormat.class, TestSupport.fastq(10, 1, true), gzip);
			int fastqMany = eventsReading(FastQInputFormat.class, TestSupport.fastq(2000, 1, true), gzip);
			assertEquals("FASTQ, gzip=" + gzip, fastqFew, fastqMany);

			int fastaFew = eventsReading(FastAInputFormat.class, TestSupport.fasta(10, 1, true), gzip);
			int fastaMany = eventsReading(FastAInputFormat.class, TestSupport.fasta(2000, 1, true), gzip);
			assertEquals("FASTA, gzip=" + gzip, fastaFew, fastaMany);

			assertTrue("a split is described with a few events", fastqMany > 0 && fastqMany <= 3);
		}
	}

	@Test
	public void nothingAboveDebugIsLoggedForValidInput() throws Exception {
		String fastq = TestSupport.fastq(500, 2, false);
		Path fq = TestSupport.write(tmp.getRoot(), "reads.fq", fastq);
		String fasta = TestSupport.fasta(500, 2, false);
		Path fa = TestSupport.write(tmp.getRoot(), "reads.fa", fasta);

		// every cut of the files, so that splits start and end at arbitrary offsets
		TestSupport.readAllSplits(FastQInputFormat.class, conf, fq, 997);
		TestSupport.readAllSplits(FastAInputFormat.class, conf, fa, 997);

		for (Level level : new Level[] { Level.INFO, Level.WARN, Level.ERROR }) {
			assertEquals(level.toString(), new ArrayList<String>(), capture.messages(level));
		}
	}

	@Test
	public void splitIsDescribedAtDebugLevel() throws Exception {
		String text = TestSupport.fastq(20, 3, true);
		Path path = TestSupport.write(tmp.getRoot(), "reads.fq", text);

		TestSupport.readWhole(FastQInputFormat.class, conf, path, text.length());

		List<String> debug = capture.messages(Level.DEBUG);
		assertEquals(debug.toString(), 2, debug.size());
		assertTrue(debug.get(0), debug.get(0).startsWith("Opened split [0, " + text.length() + ") of "));
		assertTrue(debug.get(0), debug.get(0).contains("reads.fq (codec: none)"));
		assertTrue(debug.get(1), debug.get(1).startsWith("Closed reader of "));
	}

	private List<String> warningsReading(String text) throws Exception {
		byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
		Path path = TestSupport.write(tmp.getRoot(), "truncated.fq", bytes);
		List<Record> records = TestSupport.readWhole(FastQInputFormat.class, conf, path, bytes.length);
		assertEquals("only the complete record is returned", 1, records.size());
		return capture.messages(Level.WARN);
	}

	@Test
	public void truncatedFastqRecordIsReported() throws Exception {
		List<String> warnings = warningsReading("@a\nA\n+\nI\n@b\nC\n");

		assertEquals(warnings.toString(), 1, warnings.size());
		assertTrue(warnings.get(0), warnings.get(0).contains("truncated.fq"));
		assertTrue(warnings.get(0), warnings.get(0).contains("record starting at offset 9 "));
	}

	@Test
	public void fastqRecordTruncatedAtAHeaderLikeLineIsReported() throws Exception {
		// a line starting with '@' in the middle of a record makes the reader look one line ahead
		List<String> warnings = warningsReading("@a\nA\n+\nI\n@b\nCC\n@c\n");

		assertEquals(warnings.toString(), 1, warnings.size());
		assertTrue(warnings.get(0), warnings.get(0).contains("record starting at offset 9 "));
	}
}
