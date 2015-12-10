package pluradj.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.StringSerializer;

public class StringBufferSerializer implements AttributeSerializer<StringBuffer> {
    // leverage Titan's StringSerializer to integrate with its attribute serialization flow
    private StringSerializer serializer;

    public StringBufferSerializer() {
        serializer = new StringSerializer();
    }

    @Override
    public StringBuffer read(ScanBuffer buffer) {
        String str = serializer.read(buffer);
        return new StringBuffer(str);
    }

    @Override
    public void write(WriteBuffer buffer, StringBuffer attribute) {
        serializer.write(buffer, attribute.toString());
    }
}
