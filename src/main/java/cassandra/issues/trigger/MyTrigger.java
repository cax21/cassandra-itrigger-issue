package cassandra.issues.trigger;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.triggers.ITrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MyTrigger implements ITrigger {

    private static final Logger LOG = LoggerFactory.getLogger(MyTrigger.class);

    private ThreadPoolExecutor threadPoolExecutor;

    public MyTrigger() {
        threadPoolExecutor = new ThreadPoolExecutor(2, 5,
                30, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    }

    @Override
    public Collection<Mutation> augment(Partition partition) {
        threadPoolExecutor.execute(() -> readPartition(partition));
        return Collections.emptyList();
    }


    private void readPartition(Partition partition) {
        LOG.trace("Partition {}", partition);
        long startTime = System.currentTimeMillis();
        String key = getKey(partition);
        if (!partitionIsDeleted(partition)) {
            UnfilteredRowIterator it = partition.unfilteredIterator();
            while (it.hasNext()) {
                Unfiltered un = it.next();
                if (un.isRow()) {
                    Clustering clustering = (Clustering) un.clustering();
                    Row row = partition.getRow(clustering);
                    if (!rowIsDeleted(row)) {
                        Iterator<Cell<?>> cells = row.cells().iterator();
                        Iterator<ColumnMetadata> columns = row.columns().iterator();
                        while (cells.hasNext() && columns.hasNext()) {
                            ColumnMetadata columnDef = columns.next();
                            Cell cell = cells.next();
                            if (!cell.isTombstone()) {
                                LOG.info("{} => {}", columnDef.name.toString(),
                                        CassandraTypeHelper.toJava(columnDef.type,
                                                row.getComplexColumnData(columnDef),
                                                cell));
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean partitionIsDeleted(Partition partition) {
        return partition.partitionLevelDeletion().markedForDeleteAt() > Long.MIN_VALUE;
    }

    private boolean rowIsDeleted(Row row) {
        return row.deletion().time().markedForDeleteAt() > Long.MIN_VALUE;
    }

    private String getKey(Partition partition) {
        return partition.metadata().partitionKeyType.getString(partition.partitionKey().getKey());
    }

}
