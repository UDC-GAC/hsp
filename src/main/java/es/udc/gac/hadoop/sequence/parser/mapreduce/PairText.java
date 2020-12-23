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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author Roberto Rey Exposito		<rreye@udc.es>
 * @author Luis Lorenzo Mosquera	<luis.lorenzom@udc.es>
 * @author Jorge González-Domínguez	<jgonzalezd@udc.es>
 */
public class PairText implements Writable {

	private Text left;
	private Text right;

	public PairText(Text left, Text right) {
		this.left = left;
		this.right = right;
	}

	public PairText() {
		left = null;
		right = null;
	}

	public Text getLeft() {
		return left;
	}

	public Text getRight() {
		return right;
	}

	public void setLeft(Text left) {
		this.left = left;
	}

	public void setRight(Text right) {
		this.right = right;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		left.write(out);
		right.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		left.readFields(in);
		right.readFields(in);
	}
}
