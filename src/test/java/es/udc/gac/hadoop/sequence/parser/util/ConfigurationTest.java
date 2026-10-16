package es.udc.gac.hadoop.sequence.parser.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ConfigurationTest {

	@Test
	public void defaults() {
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);

		assertEquals(64 * 1024, Configuration.getInputBufferSize(conf));
		assertTrue(Configuration.getTrimSequenceName(conf));
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
}
