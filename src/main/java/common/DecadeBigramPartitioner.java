package common;

import org.apache.hadoop.mapreduce.Partitioner;

public class DecadeBigramPartitioner extends Partitioner<DecadeBigramKey, Object> {
    @Override
    public int getPartition(DecadeBigramKey decadeBigramKey, Object o, int i) {
        return Math.abs(decadeBigramKey.getW1().hashCode()) % i;
    }
}
