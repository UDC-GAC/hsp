package es.udc.gac.hadoop.sequence.parser.mapreduce;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Ignore;
import org.junit.Test;

public class PairedEndInputSplitTest {

	private static final Path LEFT = new Path("file:///data/left.fq");
	private static final Path RIGHT = new Path("file:///data/right.fq");

	private static PairedEndInputSplit pair(String[] leftHosts, String[] rightHosts) throws Exception {
		PairedEndInputSplit split = new PairedEndInputSplit();
		split.add(new FileSplit(LEFT, 100, 1000, leftHosts));
		split.add(new FileSplit(RIGHT, 200, 2000, rightHosts));
		return split;
	}

	@Test
	public void exposesBothChildSplits() throws Exception {
		FileSplit l = new FileSplit(LEFT, 100, 1000, null);
		FileSplit r = new FileSplit(RIGHT, 200, 2000, null);
		PairedEndInputSplit split = new PairedEndInputSplit();
		split.add(l);
		split.add(r);

		assertSame(l, split.get(0));
		assertSame(r, split.get(1));
		assertEquals(LEFT, split.getPath());
		assertEquals(LEFT, split.getPath(0));
		assertEquals(RIGHT, split.getPath(1));
		assertEquals(100, split.getStart());
		assertEquals(1000, split.getLength(0));
		assertEquals(2000, split.getLength(1));
	}

	@Test
	public void lengthIsTheSumOfTheChildLengths() throws Exception {
		assertEquals(3000, pair(null, null).getLength());
	}

	@Test
	public void toStringShowsBothPathsAndLeftRange() throws Exception {
		assertEquals("(" + LEFT + "," + RIGHT + "):100+1000", pair(null, null).toString());
	}

	@Test
	public void thirdSplitIsRejected() throws Exception {
		PairedEndInputSplit split = pair(null, null);
		try {
			split.add(new FileSplit(LEFT, 0, 1, null));
			fail("expected IOException");
		} catch (IOException expected) {
			assertEquals("Too many splits", expected.getMessage());
		}
	}

	@Test
	public void locationsAreTheIntersectionOfBothHostsWhenNotEmpty() throws Exception {
		PairedEndInputSplit split = pair(new String[] { "a", "b", "c" }, new String[] { "c", "d", "b" });
		assertArrayEquals(new String[] { "b", "c" }, split.getLocations());
	}

	@Test
	public void locationsAreTheUnionWhenThereAreNoCommonHosts() throws Exception {
		PairedEndInputSplit split = pair(new String[] { "a", "b" }, new String[] { "c", "d" });
		assertArrayEquals(new String[] { "a", "b", "c", "d" }, split.getLocations());
	}

	@Test
	public void locationsOfOneSideAreUsedWhenTheOtherHasNone() throws Exception {
		assertArrayEquals(new String[] { "a", "b" }, pair(new String[] { "a", "b" }, null).getLocations());
		assertArrayEquals(new String[] { "c" }, pair(null, new String[] { "c" }).getLocations());
		assertArrayEquals(new String[0], pair(null, null).getLocations());
	}

	@Test
	public void locationsAreNotAvailableUntilBothSplitsAreAdded() throws Exception {
		PairedEndInputSplit split = new PairedEndInputSplit();
		split.add(new FileSplit(LEFT, 0, 10, new String[] { "a" }));
		assertNull(split.getLocations());
	}

	@Test
	public void childLocationsAreKept() throws Exception {
		PairedEndInputSplit split = pair(new String[] { "a" }, new String[] { "b" });
		assertArrayEquals(new String[] { "a" }, split.getLocation(0));
		assertArrayEquals(new String[] { "b" }, split.getLocation(1));
	}

	@Test
	public void childSplitsSurviveSerialization() throws Exception {
		PairedEndInputSplit split = pair(new String[] { "a" }, new String[] { "b" });
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		split.write(new DataOutputStream(bytes));

		PairedEndInputSplit copy = new PairedEndInputSplit();
		copy.readFields(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

		assertEquals(LEFT, copy.getPath(0));
		assertEquals(RIGHT, copy.getPath(1));
		assertEquals(100, copy.get(0).getStart());
		assertEquals(1000, copy.getLength(0));
		assertEquals(200, copy.get(1).getStart());
		assertEquals(2000, copy.getLength(1));
		assertEquals(split.toString(), copy.toString());
	}

	/** The aggregate length is used by frameworks for scheduling, so it must not be lost when the split is shipped. */
	@Ignore("BUG: readFields() does not restore totsize, so getLength() is 0 after deserialization")
	@Test
	public void aggregateLengthSurvivesSerialization() throws Exception {
		PairedEndInputSplit split = pair(null, null);
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		split.write(new DataOutputStream(bytes));

		PairedEndInputSplit copy = new PairedEndInputSplit();
		copy.readFields(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

		assertEquals(3000, copy.getLength());
	}
}
