package org.example.serde;

import com.google.common.base.Strings;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class AlertLevelPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int criticalLevelPartition = findCriticalPartitionNumber(cluster, topic);
        String alertLevel = ((Alert) key).getAlertLevel();
        if (isCriticalLevel(alertLevel)) {
            return criticalLevelPartition;
        } else
            return findHashPartition(cluster, topic, key, keyBytes);
    }

    private int findHashPartition(Cluster cluster, String topic, Object key, byte[] keyBytes) {
        //List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        return Utils.toPositive(Utils.murmur2(keyBytes) % partitionInfos.size());
    }

    private boolean isCriticalLevel(String alertLevel) {
        if (!Strings.isNullOrEmpty(alertLevel)
                && alertLevel.toUpperCase().contains("CRITICAL")) {
            return true;
        }
        return false;
    }

    private int findCriticalPartitionNumber(Cluster cluster, String topic) {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
