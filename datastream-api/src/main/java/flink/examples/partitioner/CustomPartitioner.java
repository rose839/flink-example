package flink.examples.partitioner;

import org.apache.flink.api.common.functions.Partitioner;

public class CustomPartitioner implements Partitioner<Long> {
    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("Number of partitions: " + numPartitions);
        if (key % 2 == 0) {
            return 0;
        } else {
            return 1;
        }
    }
}
