package es.udc.gac.hadoop.sequence.parser.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class LineReaderTest {

	private static LineReader reader(String data, int bufferSize) {
		return new LineReader(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)), bufferSize);
	}

	private static List<String> readAll(LineReader lr) throws IOException {
		List<String> lines = new ArrayList<>();
		Text t = new Text();
		while (lr.readLine(t) > 0)
			lines.add(t.toString());
		return lines;
	}

	@Test
	public void readsLinesIncludingTerminatorAndReturnsByteCount() throws IOException {
		LineReader lr = reader("abc\nde\n", 64);
		Text t = new Text();

		assertEquals(4, lr.readLine(t));
		assertEquals("abc\n", t.toString());
		assertEquals(3, lr.readLine(t));
		assertEquals("de\n", t.toString());
		assertEquals(0, lr.readLine(t));
		assertEquals("", t.toString());
	}

	@Test
	public void lastLineWithoutTerminatorIsReturned() throws IOException {
		LineReader lr = reader("abc\nde", 64);
		Text t = new Text();

		assertEquals(4, lr.readLine(t));
		assertEquals(2, lr.readLine(t));
		assertEquals("de", t.toString());
		assertEquals(0, lr.readLine(t));
	}

	@Test
	public void emptyStreamReturnsZero() throws IOException {
		Text t = new Text("previous");
		assertEquals(0, reader("", 8).readLine(t));
		assertEquals("", t.toString());
	}

	@Test
	public void blankLinesAreOneByteEach() throws IOException {
		LineReader lr = reader("\n\nx\n", 64);
		Text t = new Text();

		assertEquals(1, lr.readLine(t));
		assertEquals("\n", t.toString());
		assertEquals(1, lr.readLine(t));
		assertEquals(2, lr.readLine(t));
	}

	@Test
	public void carriageReturnIsNotATerminator() throws IOException {
		LineReader lr = reader("ab\r\ncd\r\n", 64);
		Text t = new Text();

		assertEquals(4, lr.readLine(t));
		assertEquals("ab\r\n", t.toString());
		assertEquals(4, lr.readLine(t));

		lr = reader("ab\rcd\n", 64);
		assertEquals(6, lr.readLine(t));
		assertEquals("ab\rcd\n", t.toString());
	}

	@Test
	public void sameLinesForAnyBufferSize() throws IOException {
		String data = "first line\nsecond\n\nfourth line is a bit longer than the others\nlast";
		List<String> expected = readAll(reader(data, 1 << 16));

		assertEquals(5, expected.size());
		for (int bufferSize = 1; bufferSize <= data.length() + 1; bufferSize++)
			assertEquals("buffer size " + bufferSize, expected, readAll(reader(data, bufferSize)));
	}

	@Test
	public void lineLongerThanBufferIsReadWhole() throws IOException {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1000; i++)
			sb.append('x');
		Text t = new Text();

		assertEquals(1001, reader(sb + "\n", 16).readLine(t));
		assertEquals(sb + "\n", t.toString());
	}

	@Test
	public void toleratesShortReadsFromTheUnderlyingStream() throws IOException {
		final byte[] data = "abc\ndef\nghi".getBytes(StandardCharsets.UTF_8);
		InputStream oneByteAtATime = new InputStream() {
			private int pos = 0;

			@Override
			public int read() {
				return pos < data.length ? data[pos++] : -1;
			}

			@Override
			public int read(byte[] b, int off, int len) {
				if (pos >= data.length)
					return -1;
				b[off] = data[pos++];
				return 1;
			}
		};

		List<String> lines = readAll(new LineReader(oneByteAtATime, 8));
		assertEquals(3, lines.size());
		assertEquals("abc\n", lines.get(0));
		assertEquals("ghi", lines.get(2));
	}

	@Test
	public void seekWithinBufferRereadsFromThatPosition() throws IOException {
		LineReader lr = reader("abc\ndef\n", 64);
		Text t = new Text();

		assertEquals(4, lr.readLine(t));
		assertEquals(4, lr.getPos());
		lr.seek(lr.getPos() - 4);
		assertEquals(0, lr.getPos());
		assertEquals(4, lr.readLine(t));
		assertEquals("abc\n", t.toString());
		assertEquals(4, lr.readLine(t));
		assertEquals("def\n", t.toString());
	}

	@Test
	public void seekOutsideTheBufferIsRejected() throws IOException {
		LineReader lr = reader("abc\n", 8);

		for (long pos : new long[] { -1, 8, 100 }) {
			try {
				lr.seek(pos);
				fail("expected IOException for position " + pos);
			} catch (IOException expected) {
				// ok
			}
		}
	}

	@Test
	public void trimCutsNameAtFirstSpaceAndEndsItWithLineFeed() {
		Text t = new Text("@read1 comment here\n");
		LineReader.trim(t, 2);
		assertEquals("@read1\n", t.toString());
	}

	@Test
	public void trimWithoutSpaceLeavesTextUntouched() {
		Text t = new Text("@read1\n");
		LineReader.trim(t, 2);
		assertEquals("@read1\n", t.toString());
	}

	@Test
	public void trimIgnoresSpacesBeforeStartPosition() {
		Text t = new Text("@ a b\n");
		LineReader.trim(t, 2);
		assertEquals("@ a\n", t.toString());

		t = new Text("@ ab\n");
		LineReader.trim(t, 2);
		assertEquals("@ ab\n", t.toString());
	}
}
