package cassandra.issues.trigger;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.serializers.*;
import org.apache.cassandra.transport.ProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CassandraTypeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraTypeHelper.class);

    static Object toJavaCollection(AbstractType<?> type, ValueAccessor accessor, Object value, ByteBuffer buffer,
                                   ComplexColumnData ccd) {
        if (type instanceof  MapType) {
            /*
            This code crashes
            MapSerializer<String,Long> serializer = (MapSerializer) type.getSerializer();
            Map<String, Long> m = serializer.deserialize(value, accessor);
             */
            MapType mapType = (MapType) type;
            AbstractType<?> keysType = mapType.getKeysType();
            TypeSerializer<?> keysSer = keysType.getSerializer();
            AbstractType<?> valuesType = mapType.getValuesType();
            TypeSerializer<?> valuesSer = valuesType.getSerializer();
            List<ByteBuffer> bbList = mapType.serializedValues(ccd.iterator());
            LOG.info("Number of bytesBuffer {}", bbList.size());
            Map<Object, Object> m = new HashMap<>();
            int i = 0;
            while (i < bbList.size()) {
                ByteBuffer kbb = bbList.get(i++);
                ByteBuffer vbb = bbList.get(i++);
                LOG.info("Index[{}] Key {} Value {}", i, kbb, vbb);
                m.put(keysSer.deserialize(kbb), valuesSer.deserialize(vbb));
            }
            return m;
        }
        throw new RuntimeException("Not supported so far!");
    }


    static Object toJava(AbstractType<?> type, ComplexColumnData complexColumnData, Cell cell) {
        if (type.isCollection()) {
            LOG.info("collection type {} ccd {} accessor {} value {} buffer {}={}",
                    type, complexColumnData, cell.accessor(), cell.value(), cell.buffer(),
                    type.getString(cell.buffer().array(), ByteArrayAccessor.instance));
            return toJavaCollection(type, cell.accessor(), cell.value(), cell.buffer(), complexColumnData);
        }
        return type.getString(cell.buffer());
    }
}
