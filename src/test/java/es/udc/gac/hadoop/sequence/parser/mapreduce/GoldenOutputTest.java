package es.udc.gac.hadoop.sequence.parser.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import es.udc.gac.hadoop.sequence.parser.TestSupport;
import es.udc.gac.hadoop.sequence.parser.TestSupport.Record;

/**
 * Pins the exact output (key and bytes of each value) of the readers against files under
 * src/test/resources/golden. Each line of an .expected file is "key TAB value" with line feeds
 * escaped as \n. A deliberate change of the output format must update these files.
 */
public class GoldenOutputTest {

	private Configuration conf;

	@Before
	public void setUp() {
		conf = TestSupport.newConf();
	}

	private static File resource(String name) throws Exception {
		return new File(GoldenOutputTest.class.getResource("/golden/" + name).toURI());
	}

	private void assertOutputMatches(Class<? extends SingleEndSequenceInputFormat> format, String input, String expected) throws Exception {
		File in = resource(input);
		List<Record> records = TestSupport.readWhole(format, conf, new Path(in.toURI()), in.length());

		StringBuilder actual = new StringBuilder();
		for (Record r : records)
			actual.append(r.key).append('\t').append(r.value.replace("\n", "\\n")).append('\n');

		assertEquals(new String(Files.readAllBytes(resource(expected).toPath()), StandardCharsets.UTF_8), actual.toString());
	}

	@Test
	public void fastq() throws Exception {
		assertOutputMatches(FastQInputFormat.class, "sample.fq", "sample.fq.expected");
	}

	@Test
	public void fastqWithoutNameTrimming() throws Exception {
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setTrimSequenceName(conf, false);
		assertOutputMatches(FastQInputFormat.class, "sample.fq", "sample.fq.notrim.expected");
	}

	@Test
	public void fasta() throws Exception {
		assertOutputMatches(FastAInputFormat.class, "sample.fa", "sample.fa.expected");
	}
}
