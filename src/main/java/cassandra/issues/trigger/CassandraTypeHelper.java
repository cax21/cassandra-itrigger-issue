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

    static Object toJavaCollection(AbstractType<?> type, ComplexColumnData ccd) {
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
            LOG.info("Number of bytesBuffer in map {}", bbList.size());
            Map<Object, Object> m = new HashMap<>();
            int i = 0;
            while (i < bbList.size()) {
                ByteBuffer kbb = bbList.get(i++);
                ByteBuffer vbb = bbList.get(i++);
                LOG.info("Index[{}] Key {} Value {}", i, kbb, vbb);
                m.put(keysSer.deserialize(kbb), valuesSer.deserialize(vbb));
            }
            return m;
        } else if (type instanceof ListType) {
            // let's consider the only case of time_stamps list<frozen<map<text,bigint>>>
            ListType listType = (ListType) type;
            AbstractType elemsType = listType.getElementsType();
            TypeSerializer<?> elemsSer = elemsType.getSerializer();
            List<ByteBuffer> bbList = listType.serializedValues(ccd.iterator());
            LOG.info("Number of bytesBuffer in list {}", bbList.size());
            List ret = new ArrayList();
            int i = 0;
            while (i < bbList.size()) {
                ByteBuffer bb = bbList.get(i++);
                LOG.info("Index[{}] Element {}", i, bb);
                ret.add(elemsSer.deserialize(bb));
            }
//            ListSerializer listSerializer = (ListSerializer) type.getSerializer();
//            MapSerializer mapSerializer = (MapSerializer) listSerializer.elements;
//            Map<String, Long> m = (Map<String, Long>) mapSerializer.deserialize(buffer);
//            ret.add(m);
            return ret;
        }
        throw new RuntimeException("Not supported so far!");
    }


    static Object toJava(AbstractType<?> type, ComplexColumnData complexColumnData, Cell cell) {
        if (type.isCollection()) {
            LOG.info("collection type {} ccd {} accessor {} value {} buffer {}={}",
                    type, complexColumnData, cell.accessor(), cell.value(), cell.buffer(),
                    type.getString(cell.buffer().array(), ByteArrayAccessor.instance));
            return toJavaCollection(type, complexColumnData);
        }
        return type.getString(cell.buffer());
    }
}
