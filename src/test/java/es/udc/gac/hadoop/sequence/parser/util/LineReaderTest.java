package es.udc.gac.hadoop.sequence.parser.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
	public void sameLinesAsASimpleSplitterOnRandomBytes() throws IOException {
		// bytes next to LF in value or bit pattern, to catch a faulty word-at-a-time search
		byte[] alphabet = { '\n', '\n', 0x0B, 0x09, (byte) 0x8A, 0x1A, 0x00, (byte) 0xFF, 'A', ' ' };
		java.util.Random rnd = new java.util.Random(42);

		for (int round = 0; round < 200; round++) {
			byte[] data = new byte[rnd.nextInt(300)];
			for (int i = 0; i < data.length; i++)
				data[i] = alphabet[rnd.nextInt(alphabet.length)];

			List<byte[]> expected = new ArrayList<>();
			int from = 0;
			for (int i = 0; i < data.length; i++) {
				if (data[i] == '\n') {
					expected.add(java.util.Arrays.copyOfRange(data, from, i + 1));
					from = i + 1;
				}
			}
			if (from < data.length)
				expected.add(java.util.Arrays.copyOfRange(data, from, data.length));

			for (int bufferSize : new int[] { 1, 3, 7, 8, 9, 15, 16, 17, 64, 1000 }) {
				LineReader lr = new LineReader(new ByteArrayInputStream(data), bufferSize);
				Text t = new Text();
				for (int i = 0; i < expected.size(); i++) {
					String msg = "round " + round + ", buffer size " + bufferSize + ", line " + i;
					assertEquals(msg, expected.get(i).length, lr.readLine(t));
					assertEquals(msg, new Text(expected.get(i)), t);
				}
				assertEquals(0, lr.readLine(t));
			}
		}
	}

	@Test
	public void appendLineKeepsWhatTheTextAlreadyHas() throws IOException {
		LineReader lr = reader("abc\nde\n", 64);
		Text t = new Text("x");

		assertEquals(4, lr.appendLine(t));
		assertEquals("xabc\n", t.toString());
		assertEquals(3, lr.appendLine(t));
		assertEquals("xabc\nde\n", t.toString());
		assertEquals(0, lr.appendLine(t));
		assertEquals("xabc\nde\n", t.toString());
	}

	@Test
	public void skipLineConsumesTheLineWithoutCopyingIt() throws IOException {
		LineReader lr = reader("abc\n\nde", 64);
		Text t = new Text();

		assertEquals(4, lr.skipLine());
		assertEquals(1, lr.skipLine());
		assertEquals(2, lr.readLine(t));
		assertEquals("de", t.toString());
		assertEquals(0, lr.skipLine());
	}

	@Test
	public void skipLineAcrossBuffersCountsEveryByte() throws IOException {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 100; i++)
			sb.append('x');

		for (int bufferSize : new int[] { 1, 7, 8, 64, 101 }) {
			LineReader lr = reader(sb + "\n" + sb + "\nrest\n", bufferSize);
			Text t = new Text();

			assertEquals("buffer size " + bufferSize, 101, lr.skipLine());
			assertEquals("buffer size " + bufferSize, 101, lr.skipLine());
			assertEquals("buffer size " + bufferSize, 5, lr.readLine(t));
		}
	}

	@Test
	public void peekReturnsTheFirstByteOfTheNextLineWithoutConsumingIt() throws IOException {
		LineReader lr = reader("@abc\n\u00ffx\n", 64);
		Text t = new Text();

		assertEquals('@', lr.peek());
		assertEquals('@', lr.peek());
		assertEquals(5, lr.readLine(t));
		assertEquals("@abc\n", t.toString());
		assertEquals(0xC3, lr.peek()); // UTF-8 of U+00FF, as an unsigned byte
		assertEquals(4, lr.skipLine());
		assertEquals(-1, lr.peek());
		assertEquals(-1, lr.peek());
		assertEquals(0, lr.readLine(t));
	}

	@Test
	public void peekRefillsTheBufferBetweenLines() throws IOException {
		for (int bufferSize = 1; bufferSize <= 12; bufferSize++) {
			LineReader lr = reader("ab\ncd\nef", bufferSize);
			Text t = new Text();

			for (String line : new String[] { "ab\n", "cd\n", "ef" }) {
				assertEquals("buffer size " + bufferSize, line.charAt(0), lr.peek());
				assertEquals("buffer size " + bufferSize, line.length(), lr.readLine(t));
				assertEquals("buffer size " + bufferSize, line, t.toString());
			}
			assertEquals("buffer size " + bufferSize, -1, lr.peek());
		}
	}

	@Test
	public void peekToleratesShortReadsFromTheUnderlyingStream() throws IOException {
		final byte[] data = "abc\ndef".getBytes(StandardCharsets.UTF_8);
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
		LineReader lr = new LineReader(oneByteAtATime, 8);
		Text t = new Text();

		assertEquals('a', lr.peek());
		assertEquals(4, lr.readLine(t));
		assertEquals('d', lr.peek());
		assertEquals("def", readAll(lr).get(0));
	}

	@Test
	public void mixedOperationsGiveTheSameLinesAsASimpleSplitter() throws IOException {
		byte[] alphabet = { '\n', '\n', 0x0B, (byte) 0x8A, 0x00, (byte) 0xFF, 'A', '@' };
		java.util.Random rnd = new java.util.Random(7);

		for (int round = 0; round < 200; round++) {
			byte[] data = new byte[rnd.nextInt(200)];
			for (int i = 0; i < data.length; i++)
				data[i] = alphabet[rnd.nextInt(alphabet.length)];

			List<byte[]> expected = new ArrayList<>();
			int from = 0;
			for (int i = 0; i < data.length; i++) {
				if (data[i] == '\n') {
					expected.add(java.util.Arrays.copyOfRange(data, from, i + 1));
					from = i + 1;
				}
			}
			if (from < data.length)
				expected.add(java.util.Arrays.copyOfRange(data, from, data.length));

			for (int bufferSize : new int[] { 1, 3, 8, 9, 64 }) {
				LineReader lr = new LineReader(new ByteArrayInputStream(data), bufferSize);
				Text t = new Text();
				for (int i = 0; i < expected.size(); i++) {
					String msg = "round " + round + ", buffer size " + bufferSize + ", line " + i;
					t.set("prefix");
					assertEquals(msg, expected.get(i)[0] & 0xFF, lr.peek());

					switch (rnd.nextInt(3)) {
					case 0:
						assertEquals(msg, expected.get(i).length, lr.readLine(t));
						assertEquals(msg, new Text(expected.get(i)), t);
						break;
					case 1:
						assertEquals(msg, expected.get(i).length, lr.appendLine(t));
						byte[] withPrefix = new byte[6 + expected.get(i).length];
						System.arraycopy("prefix".getBytes(StandardCharsets.US_ASCII), 0, withPrefix, 0, 6);
						System.arraycopy(expected.get(i), 0, withPrefix, 6, expected.get(i).length);
						assertEquals(msg, new Text(withPrefix), t);
						break;
					default:
						assertEquals(msg, expected.get(i).length, lr.skipLine());
					}
				}
				assertEquals(-1, lr.peek());
				assertEquals(0, lr.skipLine());
			}
		}
	}

	@Test
	public void appendTwoLinesAppendsBothWhenTheyAreCompleteInTheBuffer() throws IOException {
		LineReader lr = reader("ab\ncd\nef\n", 64);
		Text t = new Text("x");

		lr.peek(); // fills the buffer
		assertEquals(6, lr.appendTwoLines(t, '@'));
		assertEquals("xab\ncd\n", t.toString());
		assertEquals(3, lr.readLine(t));
		assertEquals("ef\n", t.toString());
	}

	@Test
	public void appendTwoLinesConsumesNothingIfTheSecondLineStartsWithTheExcludedByte() throws IOException {
		LineReader lr = reader("ab\n@cd\n", 64);
		Text t = new Text();

		lr.peek();
		assertEquals(0, lr.appendTwoLines(t, '@'));
		assertEquals("", t.toString());
		assertEquals(3, lr.readLine(t));
		assertEquals("ab\n", t.toString());
		assertEquals("@cd\n", readAll(lr).get(0));
	}

	@Test
	public void appendTwoLinesConsumesNothingIfALineIsMissingOrNotTerminated() throws IOException {
		for (String data : new String[] { "", "abcd", "ab\n", "ab\ncd" }) {
			LineReader lr = reader(data, 64);
			Text t = new Text();

			lr.peek();
			assertEquals(data, 0, lr.appendTwoLines(t, '@'));
			assertEquals(data, "", t.toString());
			assertEquals(data, data, String.join("", readAll(lr)));
		}
	}

	@Test
	public void appendTwoLinesDoesNotRefillTheBuffer() throws IOException {
		LineReader lr = reader("ab\ncd\nef\n", 64);
		Text t = new Text();

		assertEquals(0, lr.appendTwoLines(t, '@')); // nothing buffered yet
		assertEquals("", t.toString());
		assertEquals(3, lr.readLine(t));

		// the second line is cut by the end of the buffer
		lr = reader("ab\ncd\nef\n", 5);
		lr.peek();
		assertEquals(0, lr.appendTwoLines(t, '@'));
		assertEquals(3, lr.readLine(t));
		assertEquals("ab\n", t.toString());
	}

	@Test
	public void appendTwoLinesGivesTheSameLinesAsAppendLine() throws IOException {
		byte[] alphabet = { '\n', '\n', 0x0B, (byte) 0x8A, 0x00, 'A', '@' };
		java.util.Random rnd = new java.util.Random(11);
		int together = 0;

		for (int round = 0; round < 300; round++) {
			byte[] data = new byte[rnd.nextInt(200)];
			for (int i = 0; i < data.length; i++)
				data[i] = alphabet[rnd.nextInt(alphabet.length)];

			for (int bufferSize : new int[] { 3, 8, 9, 64, 1000 }) {
				LineReader both = new LineReader(new ByteArrayInputStream(data), bufferSize);
				LineReader single = new LineReader(new ByteArrayInputStream(data), bufferSize);
				Text a = new Text(), b = new Text(), x = new Text(), y = new Text();

				while (both.peek() >= 0) {
					String msg = "round " + round + ", buffer size " + bufferSize;
					a.clear();
					int n = both.appendTwoLines(a, '@');

					if (n > 0) {
						together++;
						x.clear();
						y.clear();
						assertEquals(msg, n, single.appendLine(x) + single.appendLine(y));
						assertTrue(msg, (y.getBytes()[0] & 0xFF) != '@');
						byte[] expected = new byte[n];
						System.arraycopy(x.getBytes(), 0, expected, 0, x.getLength());
						System.arraycopy(y.getBytes(), 0, expected, x.getLength(), y.getLength());
						assertEquals(msg, new Text(expected), a);
					} else {
						assertEquals(msg, "", a.toString());
						assertEquals(msg, single.readLine(y), both.readLine(b));
						assertEquals(msg, y, b);
					}
				}
				assertEquals(0, single.readLine(y));
			}
		}
		assertTrue("the test did not exercise appendTwoLines()", together > 100);
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

	@Test
	public void trimIgnoresSpacesPastTheLengthOfTheText() {
		// the backing array of a reused Text keeps the bytes of previous, longer lines
		Text t = new Text("@read1 comment here\n");
		t.set("@r\n".getBytes(), 0, 3);
		LineReader.trim(t, 2);
		assertEquals("@r\n", t.toString());
	}

	@Test
	public void trimWithStartBeyondTheLengthLeavesTextUntouched() {
		Text t = new Text("@");
		LineReader.trim(t, 2);
		assertEquals("@", t.toString());
	}
}
