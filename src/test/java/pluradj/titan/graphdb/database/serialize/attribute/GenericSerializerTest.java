package pluradj.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.StandardSerializer;
import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.*;

public class GenericSerializerTest {

    private StandardSerializer getStandardSerializer() {
        // register the custom attribute serializer with the Titan standard serializer
        StandardSerializer serialize = new StandardSerializer();
        serialize.registerClass(2, HashSet.class, new GenericSerializer<HashSet>());
        assertTrue(serialize.validDataType(HashSet.class));
        return serialize;
    }

    @Test
    public void writeReadObjectNotNull() {
        StandardSerializer serialize = getStandardSerializer();

        // use the serializer to write the object
        HashSet<String> set = new HashSet<>();
        set.add("one");
        DataOutput out = serialize.getDataOutput(128);
        out.writeObjectNotNull(set);

        // use the serializer to read the object
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        HashSet<String> read = (HashSet<String>) serialize.readObjectNotNull(b, HashSet.class);

        // make sure they are equal
        assertEquals(read.size(), set.size());
        assertEquals(read.iterator().next(), set.iterator().next());
    }

    @Test
    public void writeReadClassAndObject() {
        StandardSerializer serialize = getStandardSerializer();

        // use the serializer to write the object
        HashSet<String> set = new HashSet<>();
        set.add("one");
        DataOutput out = serialize.getDataOutput(128);
        out.writeClassAndObject(set);

        // use the serializer to read the object
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        HashSet<String> read = (HashSet<String>) serialize.readClassAndObject(b);

        // make sure they are equal
        assertEquals(read.size(), set.size());
        assertEquals(read.iterator().next(), set.iterator().next());
    }

    @Test
    public void writeReadObject() {
        StandardSerializer serialize = getStandardSerializer();

        // use the serializer to write the object
        HashSet<String> set = new HashSet<>();
        set.add("one");
        DataOutput out = serialize.getDataOutput(128);
        out.writeObject(set, HashSet.class);

        // use the serializer to read the object
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        HashSet<String> read = (HashSet<String>) serialize.readObject(b, HashSet.class);

        // make sure they are equal
        assertEquals(read.size(), set.size());
        assertEquals(read.iterator().next(), set.iterator().next());
    }

}
