package cassandra.issues.trigger;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.serializers.*;
import org.apache.cassandra.transport.ProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CassandraTypeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraTypeHelper.class);

    static Object toJavaCollection(AbstractType<?> type, ValueAccessor accessor, Object value, ByteBuffer buffer) {
        LOG.info("collection {} accessor {} value {} buffer {}={}", type, accessor, value, buffer, type.getString(buffer.array(), ByteArrayAccessor.instance));
        if (type instanceof  MapType) {
            ProtocolVersion version = ProtocolVersion.V3;
            MapSerializer<String,Long> serializer = (MapSerializer) type.getSerializer();
            Map<String, Long> m = serializer.deserialize(value, accessor);
            return m;
        }
        throw new RuntimeException("Not supported so far!");
    }


    static Object toJava(AbstractType<?> type, Cell cell) {
        if (type.isCollection()) {
            return toJavaCollection(type, cell.accessor(), cell.value(), cell.buffer());
        }
        return type.getString(cell.buffer());
    }
}
