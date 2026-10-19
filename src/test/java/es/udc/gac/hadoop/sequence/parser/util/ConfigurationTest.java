package es.udc.gac.hadoop.sequence.parser.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import es.udc.gac.hadoop.sequence.parser.TestSupport;
import es.udc.gac.hadoop.sequence.parser.mapreduce.FastAInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.FastQInputFormat;

public class ConfigurationTest {

	@Test
	public void defaults() {
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);

		assertEquals(64 * 1024, Configuration.getInputBufferSize(conf));
		assertFalse(Configuration.getTrimSequenceName(conf));
	}

	@Test
	public void settersAreReadBackAndUseDocumentedKeys() {
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);

		Configuration.setInputBufferSize(conf, 1234);
		Configuration.setTrimSequenceName(conf, false);

		assertEquals(1234, Configuration.getInputBufferSize(conf));
		assertFalse(Configuration.getTrimSequenceName(conf));
		assertEquals("1234", conf.get("hsp.input.buffer.size"));
		assertEquals("false", conf.get("hsp.trim.sequence.name"));
	}

	private static void assertRejected(String messagePart, Runnable action) {
		try {
			action.run();
			fail("IllegalArgumentException expected");
		} catch (IllegalArgumentException e) {
			assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
		}
	}

	@Test
	public void bufferSizeMustBePositive() {
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);

		for (String value : new String[] { "0", "-1" }) {
			conf.set("hsp.input.buffer.size", value);
			assertRejected("hsp.input.buffer.size", () -> Configuration.getInputBufferSize(conf));
			assertRejected(value, () -> Configuration.getInputBufferSize(conf));
		}
		assertRejected("hsp.input.buffer.size", () -> Configuration.setInputBufferSize(conf, 0));
		assertRejected("hsp.input.buffer.size", () -> Configuration.setInputBufferSize(conf, -5));
		conf.set("hsp.input.buffer.size", "1");
		assertEquals(1, Configuration.getInputBufferSize(conf));
	}

	@Test
	public void bufferSizeMustBeANumber() {
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);
		conf.set("hsp.input.buffer.size", "64k");

		assertRejected("hsp.input.buffer.size", () -> Configuration.getInputBufferSize(conf));
		assertRejected("64k", () -> Configuration.getInputBufferSize(conf));
	}

	@Test
	public void trimMustBeTrueOrFalse() {
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);

		conf.set("hsp.trim.sequence.name", "maybe");
		assertRejected("hsp.trim.sequence.name", () -> Configuration.getTrimSequenceName(conf));
		assertRejected("maybe", () -> Configuration.getTrimSequenceName(conf));
		conf.set("hsp.trim.sequence.name", "yes");
		assertRejected("yes", () -> Configuration.getTrimSequenceName(conf));
	}

	@Test
	public void trimAcceptsAnyCaseAndSurroundingSpaces() {
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);

		conf.set("hsp.trim.sequence.name", " TRUE ");
		assertTrue(Configuration.getTrimSequenceName(conf));
		conf.set("hsp.trim.sequence.name", "False");
		assertFalse(Configuration.getTrimSequenceName(conf));
		conf.set("hsp.trim.sequence.name", "");
		assertFalse(Configuration.getTrimSequenceName(conf));
	}

	@Test
	public void readersRejectAnInvalidConfigurationWhenTheyAreCreated() throws Exception {
		org.apache.hadoop.conf.Configuration conf = TestSupport.newConf();
		conf.set("hsp.input.buffer.size", "0");
		assertRejectedReader(new FastQInputFormat(), conf, "hsp.input.buffer.size");
		assertRejectedReader(new FastAInputFormat(), conf, "hsp.input.buffer.size");

		conf = TestSupport.newConf();
		conf.set("hsp.trim.sequence.name", "maybe");
		assertRejectedReader(new FastQInputFormat(), conf, "hsp.trim.sequence.name");
		assertRejectedReader(new FastAInputFormat(), conf, "hsp.trim.sequence.name");
	}

	private static void assertRejectedReader(org.apache.hadoop.mapreduce.lib.input.FileInputFormat<?, ?> format,
			org.apache.hadoop.conf.Configuration conf, String messagePart) throws Exception {
		try {
			format.createRecordReader(null, TestSupport.context(conf));
			fail("IllegalArgumentException expected");
		} catch (IllegalArgumentException e) {
			assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
		}
	}
}
