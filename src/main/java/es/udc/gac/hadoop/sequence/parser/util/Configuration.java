/*
 * Copyright (C) 2017 Universidade da Coruña
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

public final class Configuration {

	private static final String INPUT_BUFFER_SIZE = "hdfs.sequence.parser.buffer.size";
	private static final int INPUT_BUFFER_SIZE_DEFAULT = 8192;

	public static int getInputBufferSize(org.apache.hadoop.conf.Configuration conf) {
		return conf.getInt(INPUT_BUFFER_SIZE, INPUT_BUFFER_SIZE_DEFAULT);
	}

	public static void setInputBufferSize(org.apache.hadoop.conf.Configuration conf, int bufferSize) {
		conf.setInt(INPUT_BUFFER_SIZE, bufferSize);
	}
}
