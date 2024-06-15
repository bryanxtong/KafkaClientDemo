package org.example.sticky;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class MyStickyPartitioner implements Partitioner {
    private long lastPartitionChangeTimeMillis = 0L;
    private int currentPosition = -1;
    private long partitionChangeTimeGap = 100L;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (keyBytes == null) {
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            int availablePartitionsSize = availablePartitions.size();
            if (availablePartitionsSize > 0) {
                handlePartionChange(availablePartitionsSize);
                return availablePartitions.get(currentPosition).partition();
            } else {
                handlePartionChange(numPartitions);
                return currentPosition;
            }
        } else {
            return Utils.toPositive(Utils.murmur2(keyBytes) % numPartitions);
        }

    }

    /**
     * select one partition Randomly after gap expires
     * @param partitionNum
     */
    private void handlePartionChange(int partitionNum) {
        long currentTimeMillis = System.currentTimeMillis();
        if (currentPosition < 0
                || currentPosition >= partitionNum
                || currentTimeMillis - lastPartitionChangeTimeMillis >= partitionChangeTimeGap) {
            lastPartitionChangeTimeMillis = currentTimeMillis;
            currentPosition = Utils.toPositive(ThreadLocalRandom.current().nextInt(partitionNum) % partitionNum);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
