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

/**
 * 
 * @author Roberto Rey Exposito		<rreye@udc.es>
 * @author Luis Lorenzo Mosquera	<luis.lorenzom@udc.es> 
 * @author Jorge González-Domínguez	<jgonzalezd@udc.es>
 */
public final class Configuration {

	/**
	 * Size in bytes of the input buffer of the readers, which is also the initial size of the
	 * {@code Text} that holds each record. Must be a positive integer. Default: 65536.
	 */
	public static final String INPUT_BUFFER_SIZE_KEY = "hsp.input.buffer.size";
	private static final int INPUT_BUFFER_SIZE_DEFAULT = 64*1024;
	/**
	 * Whether the read name is cut at its first space, dropping the description that follows it.
	 * Must be {@code true} or {@code false}. Default: {@code false} (the name is kept as it is in the file).
	 */
	public static final String TRIM_SEQUENCE_NAME_KEY = "hsp.trim.sequence.name";
	private static final boolean TRIM_SEQUENCE_NAME_DEFAULT = false;

	/**
	 * @throws IllegalArgumentException if the property is not a positive integer
	 */
	public static int getInputBufferSize(org.apache.hadoop.conf.Configuration conf) {
		int bufferSize;
		try {
			bufferSize = conf.getInt(INPUT_BUFFER_SIZE_KEY, INPUT_BUFFER_SIZE_DEFAULT);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(INPUT_BUFFER_SIZE_KEY + " must be a positive integer, got '"
					+ conf.get(INPUT_BUFFER_SIZE_KEY) + "'", e);
		}
		checkBufferSize(bufferSize);
		return bufferSize;
	}

	/**
	 * @throws IllegalArgumentException if {@code bufferSize} is not positive
	 */
	public static void setInputBufferSize(org.apache.hadoop.conf.Configuration conf, int bufferSize) {
		checkBufferSize(bufferSize);
		conf.setInt(INPUT_BUFFER_SIZE_KEY, bufferSize);
	}

	public static void setTrimSequenceName(org.apache.hadoop.conf.Configuration conf, boolean trimSequenceName) {
		conf.setBoolean(TRIM_SEQUENCE_NAME_KEY, trimSequenceName); 
	}

	/**
	 * @throws IllegalArgumentException if the property is set to something other than {@code true} or {@code false}
	 */
	public static boolean getTrimSequenceName(org.apache.hadoop.conf.Configuration conf) {
		String value = conf.getTrimmed(TRIM_SEQUENCE_NAME_KEY);
		if (value == null || value.isEmpty())
			return TRIM_SEQUENCE_NAME_DEFAULT;
		if (value.equalsIgnoreCase("true"))
			return true;
		if (value.equalsIgnoreCase("false"))
			return false;
		throw new IllegalArgumentException(TRIM_SEQUENCE_NAME_KEY + " must be true or false, got '" + value + "'");
	}

	private static void checkBufferSize(int bufferSize) {
		if (bufferSize <= 0)
			throw new IllegalArgumentException(INPUT_BUFFER_SIZE_KEY + " must be a positive integer, got " + bufferSize);
	}
}
