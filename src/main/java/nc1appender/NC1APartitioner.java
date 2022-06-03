package nc1appender;

import common.DecadeBigramKey;
import common.DecadeBigramValue;
import org.apache.hadoop.mapreduce.Partitioner;

public class NC1APartitioner extends Partitioner<DecadeBigramKey, DecadeBigramValue> {

    @Override
    public int getPartition(DecadeBigramKey decadeBigramKey, DecadeBigramValue decadeBigramValue, int i) {
        return decadeBigramKey.getW1().hashCode() % i;
    }
}
