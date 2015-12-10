package pluradj.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.StandardSerializer;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class ArrayListSerializerTest {

    private StandardSerializer getStandardSerializer() {
        // register the custom attribute serializer with the Titan standard serializer
        StandardSerializer serialize = new StandardSerializer();
        serialize.registerClass(2, ArrayList.class, new ArrayListSerializer());
        assertTrue(serialize.validDataType(ArrayList.class));
        return serialize;
    }

    @Test
    public void writeReadObjectNotNull() {
        StandardSerializer serialize = getStandardSerializer();

        // use the serializer to write the object
        ArrayList<String> arrayList = new ArrayList<String>();
        arrayList.add("one");
        DataOutput out = serialize.getDataOutput(128);
        out.writeObjectNotNull(arrayList);

        // use the serializer to read the object
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        ArrayList<String> read = (ArrayList<String>) serialize.readObjectNotNull(b, ArrayList.class);

        // make sure they are equal
        assertEquals(arrayList.size(), read.size());
        assertEquals(arrayList.get(0), read.get(0));
    }

    @Test
    public void writeReadClassAndObject() {
        StandardSerializer serialize = getStandardSerializer();

        // use the serializer to write the object
        ArrayList<String> arrayList = new ArrayList<String>();
        arrayList.add("one");
        DataOutput out = serialize.getDataOutput(128);
        out.writeClassAndObject(arrayList);

        // use the serializer to read the object
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        ArrayList<String> read = (ArrayList<String>) serialize.readClassAndObject(b);

        // make sure they are equal
        assertEquals(arrayList.size(), read.size());
        assertEquals(arrayList.get(0), read.get(0));
    }

    @Test
    public void writeReadObject() {
        StandardSerializer serialize = getStandardSerializer();

        // use the serializer to write the object
        ArrayList<String> arrayList = new ArrayList<String>();
        arrayList.add("one");
        DataOutput out = serialize.getDataOutput(128);
        out.writeObject(arrayList, ArrayList.class);

        // use the serializer to read the object
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        ArrayList<String> read = (ArrayList<String>) serialize.readObject(b, ArrayList.class);

        // make sure they are equal
        assertEquals(arrayList.size(), read.size());
        assertEquals(arrayList.get(0), read.get(0));
    }

}
