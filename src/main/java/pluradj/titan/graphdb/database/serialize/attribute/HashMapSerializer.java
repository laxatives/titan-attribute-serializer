package pluradj.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.ByteArraySerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.HashMap;

public class HashMapSerializer implements AttributeSerializer<HashMap> {
    // use Java object serialization to convert HashMap into byte[]
    // then leverage Titan's ByteArraySerializer to integrate with its attribute serialization flow
    private ByteArraySerializer serializer;

    public HashMapSerializer() {
        serializer = new ByteArraySerializer();
    }

    @Override
    public HashMap read(ScanBuffer buffer) {
        HashMap attribute = null;
        byte[] data = serializer.read(buffer);
        // http://docs.oracle.com/javase/8/docs/technotes/guides/language/try-with-resources.html
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bais);
        ) {
            attribute = (HashMap) ois.readObject();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return attribute;
    }

    @Override
    public void write(WriteBuffer buffer, HashMap attribute) {
        try (
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
        ) {
            oos.writeObject(attribute);
            byte[] data = baos.toByteArray();
            serializer.write(buffer, data);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
