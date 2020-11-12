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

	private static final String INPUT_BUFFER_SIZE = "hsp.input.buffer.size";
	private static final int INPUT_BUFFER_SIZE_DEFAULT = 64*1024;
	private static final String TRIM_SEQUENCE_NAME = "hsp.trim.sequence.name";
	private static final boolean TRIM_SEQUENCE_NAME_DEFAULT = true;

	public static int getInputBufferSize(org.apache.hadoop.conf.Configuration conf) {
		return conf.getInt(INPUT_BUFFER_SIZE, INPUT_BUFFER_SIZE_DEFAULT);
	}

	public static void setInputBufferSize(org.apache.hadoop.conf.Configuration conf, int bufferSize) {
		conf.setInt(INPUT_BUFFER_SIZE, bufferSize);
	}

	public static void setTrimSequenceName(org.apache.hadoop.conf.Configuration conf, boolean trimSequenceName) {
		conf.setBoolean(TRIM_SEQUENCE_NAME, trimSequenceName); 
	}

	public static boolean getTrimSequenceName(org.apache.hadoop.conf.Configuration conf) {
		return conf.getBoolean(TRIM_SEQUENCE_NAME, TRIM_SEQUENCE_NAME_DEFAULT); 
	}
}
