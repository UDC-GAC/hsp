package es.udc.gac.hadoop.sequence.parser.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class PairTextTest {

	private static byte[] serialize(Writable w) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		w.write(new DataOutputStream(bytes));
		return bytes.toByteArray();
	}

	@Test
	public void constructorsAndAccessors() {
		Text l = new Text("left"), r = new Text("right");
		PairText pair = new PairText(l, r);

		assertSame(l, pair.getLeft());
		assertSame(r, pair.getRight());

		PairText empty = new PairText();
		assertNull(empty.getLeft());
		assertNull(empty.getRight());

		empty.setLeft(l);
		empty.setRight(r);
		assertSame(l, empty.getLeft());
		assertSame(r, empty.getRight());
	}

	@Test
	public void writeAndReadFieldsRoundTrip() throws IOException {
		PairText original = new PairText(new Text("@r1\nACGT\n+\nIIII\n"), new Text("@r1\nTGCA\n+\nHHHH\n"));
		byte[] bytes = serialize(original);

		PairText copy = new PairText(new Text(), new Text());
		copy.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));

		assertEquals(original.getLeft(), copy.getLeft());
		assertEquals(original.getRight(), copy.getRight());
	}

	@Test
	public void emptyTextsRoundTrip() throws IOException {
		byte[] bytes = serialize(new PairText(new Text(), new Text()));

		PairText copy = new PairText(new Text("x"), new Text("y"));
		copy.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));

		assertEquals(0, copy.getLeft().getLength());
		assertEquals(0, copy.getRight().getLength());
	}

	/**
	 * Hadoop deserializes Writables by instantiating them through the no-arg constructor
	 * and calling readFields(), so a PairText must survive that.
	 */
	@Test
	public void canBeDeserializedThroughNoArgConstructor() throws IOException {
		byte[] bytes = serialize(new PairText(new Text("left"), new Text("right")));

		PairText copy = ReflectionUtils.newInstance(PairText.class, null);
		copy.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));

		assertEquals(new Text("left"), copy.getLeft());
		assertEquals(new Text("right"), copy.getRight());
	}

	@Test
	public void deserializedPairCanBeReusedForTheNextRecord() throws IOException {
		PairText pair = new PairText();
		pair.readFields(new DataInputStream(new ByteArrayInputStream(serialize(new PairText(new Text("l1"), new Text("r1"))))));
		pair.readFields(new DataInputStream(new ByteArrayInputStream(serialize(new PairText(new Text("l2"), new Text("r2"))))));

		assertEquals(new Text("l2"), pair.getLeft());
		assertEquals(new Text("r2"), pair.getRight());
	}

	@Test
	public void roundTripsThroughHadoopSerializationFramework() throws IOException {
		org.apache.hadoop.io.serializer.SerializationFactory factory = new org.apache.hadoop.io.serializer.SerializationFactory(new org.apache.hadoop.conf.Configuration(false));
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		org.apache.hadoop.io.serializer.Serializer<PairText> serializer = factory.getSerializer(PairText.class);
		serializer.open(bytes);
		serializer.serialize(new PairText(new Text("left"), new Text("right")));
		serializer.close();

		org.apache.hadoop.io.serializer.Deserializer<PairText> deserializer = factory.getDeserializer(PairText.class);
		deserializer.open(new ByteArrayInputStream(bytes.toByteArray()));
		PairText copy = deserializer.deserialize(null);
		deserializer.close();

		assertEquals(new Text("left"), copy.getLeft());
		assertEquals(new Text("right"), copy.getRight());
	}
}
