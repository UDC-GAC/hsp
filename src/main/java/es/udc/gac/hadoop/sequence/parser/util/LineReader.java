/*
 * Copyright (C) 2020 Universidade da Coruña
 * 
 * This file is part of HSP.
 * 
 * HSP is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * HSP is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with HSP. If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.hadoop.sequence.parser.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import org.apache.hadoop.io.Text;

/**
 * This class has been adapted from the Hadoop's LineReader implementation
 * 
 * @author Roberto Rey Exposito		<rreye@udc.es>
 * @author Luis Lorenzo Mosquera	<luis.lorenzom@udc.es>
 * @author Jorge González-Domínguez	<jgonzalezd@udc.es> 
 */
public class LineReader implements Closeable {

	/**
	 *  Current implementation only consider Line Feed (LF, '\n', 0x0A) as the
	 *  the newline character. So, it only works on Unix-like systems.
	 */
	public static final Text LF = new Text("\n");
	private static final byte LF_BYTE = '\n';
	private static final VarHandle WORD = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
	private static final long LF_WORD = 0x0A0A0A0A0A0A0A0AL;
	private static final long LOW_BITS = 0x0101010101010101L;
	private static final long HIGH_BITS = 0x8080808080808080L;

	private InputStream inputStream;
	private byte[] buffer;
	private int bufferLength; // the number of bytes of real data in the buffer
	private int bufferPos; 	// the current position in the buffer

	public LineReader(InputStream inputStream, int bufferSize) {
		this.inputStream = inputStream;
		this.buffer = new byte[bufferSize];
		this.bufferPos = 0;
		this.bufferLength = 0;
	}

	@Override
	public void close() throws IOException {
		inputStream.close();
	}

	/**
	 * Seek to the given offset.
	 *
	 * @param pos offset to seek to
	 *
	 * @throws IOException
	 */
	public void seek(long pos) throws IOException {
		if (pos < 0 || pos >= buffer.length)
			throw new IOException("Incorrect position: "+pos);

		this.bufferPos = (int) pos;
	}

	/**
	 * Return the current offset from the start of the buffer.
	 */
	public long getPos() {
		return bufferPos;
	}

	/**
	 * Read a line from the InputStream terminated by LF into the given Text.
	 *
	 * @param str the object to store the given line (including LF) 
	 *
	 * @return the number of bytes read (including LF)
	 *
	 * @throws IOException
	 */
	public int readLine(Text str) throws IOException {
		/* We're reading data from in, but the head of the stream may be
		 * already buffered, so we have two cases:
		 * 1. No newline characters are in the buffer, so we need to copy
		 *    everything and read another buffer from the stream.
		 * 2. An unambiguously terminated line is in buffer, so we just
		 *    copy to str.
		 */
		boolean newLine = false;
		long bytesConsumed = 0;
		int readLength = 0;
		int startPos = 0;
		str.clear();

		do {
			startPos = bufferPos; //starting from where we left off the last time
			if (bufferPos >= bufferLength) {
				startPos = bufferPos = 0;
				bufferLength = inputStream.read(buffer);

				if (bufferLength <= 0) {
					break; // EOF
				}
			}

			final byte[] buf = buffer;
			final int length = bufferLength;
			int p = bufferPos;

			for (; p + Long.BYTES <= length; p += Long.BYTES) { //search for newline, a word at a time
				long w = (long) WORD.get(buf, p) ^ LF_WORD;
				long lf = (w - LOW_BITS) & ~w & HIGH_BITS; // 0x80 in the first byte that was LF
				if (lf != 0) {
					p += Long.numberOfTrailingZeros(lf) >>> 3;
					newLine = true;
					break;
				}
			}

			if (!newLine) {
				for (; p < length; ++p) {
					if (buf[p] == LF_BYTE) {
						newLine = true;
						break;
					}
				}
			}

			if (newLine)
				++p; // at next invocation proceed from following byte

			bufferPos = p;
			readLength = bufferPos - startPos;
			bytesConsumed += readLength;
			str.append(buffer, startPos, readLength);
		} while (!newLine);

		return (int) bytesConsumed;
	}

	/**
	 * Finds the first blank space in the backing
	 * buffer of <code>text</code> starting as position 
	 * <code>start</code>, and then trims <code>text</code>.
	 * The starting position is measured in bytes.
	 * 
	 * @param text Text to trim
	 * @param start Starting byte position
	 */
	public static void trim(Text text, int start) {
		byte[] bytes = text.getBytes();
		int length = text.getLength();

		for (int pos = start; pos < length; pos++) {
			if (bytes[pos] == ' ') {
				bytes[pos] = LF_BYTE;
				text.set(bytes, 0, pos + 1);
				return;
			}
		}
	}
}
