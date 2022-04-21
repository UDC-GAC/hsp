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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * This InputSplit contains two childs FileSplits.
 */
public class PairedEndInputSplit extends FileSplit implements Writable {

	private static final int LENGTH = 2;

	private int fill = 0;
	private long totsize = 0L;
	private FileSplit[] splits;
	private List<List<String>> allHosts;
	private String[] hosts;

	public PairedEndInputSplit() {
		splits = new FileSplit[LENGTH];
		allHosts = new ArrayList<List<String>>(LENGTH);

		for (int i = 0; i < LENGTH; i++)
			allHosts.add(new ArrayList<String>());
	}

	/**
	 * Add an FileSplit.
	 * @throws IOException If capacity has been reached.
	 */
	public void add(FileSplit s) throws IOException, InterruptedException {
		if (null == splits) {
			throw new IOException("Uninitialized PairedEndInputSplit[]");
		}
		if (fill == splits.length) {
			throw new IOException("Too many splits");
		}

		splits[fill] = s;
		totsize += s.getLength();

		String[] hints = s.getLocations();

		if (hints != null && hints.length > 0) {
			for (String host : hints) {
				allHosts.get(fill).add(host);
			}
		}

		fill++;

		if (fill == LENGTH) {
			List<String> intersect = allHosts.get(0).stream()
					.filter(allHosts.get(1)::contains)
					.collect(Collectors.toList());

			hosts = intersect.toArray(new String[intersect.size()]);
		}
	}

	/**
	 * Get ith child FileSplit.
	 */
	public FileSplit get(int i) {
		return splits[i];
	}

	/**
	 * The file containing this split's data.
	 */
	@Override
	public Path getPath() {
		return splits[0].getPath();
	}

	/**
	 * Get the path of ith child FileSplit.
	 */
	public Path getPath(int i) {
		return splits[i].getPath();
	}

	/**
	 * The position of the first byte in the file to process.
	 */
	@Override
	public long getStart() {
		return splits[0].getStart();
	}

	/**
	 * Return the aggregate length of all child InputSplits currently added.
	 */
	@Override
	public long getLength() {
		return totsize;
	}

	/**
	 * Get the length of ith child FileSplit.
	 */
	public long getLength(int i) {
		return splits[i].getLength();
	}

	@Override
	public String toString() {
		return "(" + getPath(0) + "," + getPath(1) + "):" + getStart() + "+" + getLength(0);
	}

	/**
	 * Collect a set of hosts from all child FileSplits.
	 */
	@Override
	public String[] getLocations() throws IOException {
		if (hosts.length > 0)
			return hosts;

		HashSet<String> hostsSet = new HashSet<String>();

		for (int i = 0; i < splits.length; i++) {
			String[] hints = splits[i].getLocations();

			if (hints != null && hints.length > 0) {
				for (String host : hints) {
					hostsSet.add(host);
				}
			}
		}

		return hostsSet.toArray(new String[hostsSet.size()]);
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
