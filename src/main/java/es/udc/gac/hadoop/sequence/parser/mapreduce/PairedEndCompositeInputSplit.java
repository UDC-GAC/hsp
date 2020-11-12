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
package es.udc.gac.hadoop.sequence.parser.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * This InputSplit contains two childs FileSplits.
 */
public class PairedEndCompositeInputSplit extends InputSplit implements Writable {

	private static final int LENGTH = 2;

	private int fill = 0;
	private long totsize = 0L;
	private FileSplit[] splits;

	public PairedEndCompositeInputSplit() {
		splits = new FileSplit[LENGTH];
	}

	/**
	 * Add an FileSplit.
	 * @throws IOException If capacity has been reached.
	 */
	public void add(FileSplit s) throws IOException, InterruptedException {
		if (null == splits) {
			throw new IOException("Uninitialized FileSplit[]");
		}
		if (fill == splits.length) {
			throw new IOException("Too many splits");
		}
		splits[fill++] = s;
		totsize += s.getLength();
	}

	/**
	 * Get ith child FileSplit.
	 */
	public FileSplit get(int i) {
		return splits[i];
	}

	/**
	 * Return the aggregate length of all child InputSplits currently added.
	 */
	@Override
	public long getLength() throws IOException {
		return totsize;
	}

	/**
	 * Get the length of ith child FileSplit.
	 */
	public long getLength(int i) throws IOException, InterruptedException {
		return splits[i].getLength();
	}

	/**
	 * Collect a set of hosts from all child FileSplits.
	 */
	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		HashSet<String> hosts = new HashSet<String>();
		for (InputSplit s : splits) {
			String[] hints = s.getLocations();
			if (hints != null && hints.length > 0) {
				for (String host : hints) {
					hosts.add(host);
				}
			}
		}
		return hosts.toArray(new String[hosts.size()]);
	}

	/**
	 * getLocations from ith FileSplit.
	 */
	public String[] getLocation(int i) throws IOException, InterruptedException {
		return splits[i].getLocations();
	}

	/**
	 * Write FileSplits in the following format.
	 * {@code
	 * <split1><split2>
	 * }
	 */
	public void write(DataOutput out) throws IOException {
		for (FileSplit s : splits)
			s.write(out);
	}

	/**
	 * {@inheritDoc}
	 * @throws IOException If the child FileSplit cannot be read, typically
	 *                     for failing access checks.
	 */
	public void readFields(DataInput in) throws IOException {
		if (splits == null) {
			splits = new FileSplit[LENGTH];
		}

		for (int i = 0; i < splits.length; ++i) {
			splits[i] = new FileSplit();
			splits[i].readFields(in);
		}
	}
}
