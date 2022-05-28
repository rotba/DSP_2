package likelihood;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class LKLPartitioner extends Partitioner<LikelihoodAndDecadeBigramKey, DoubleWritable> {
    @Override
    public int getPartition(LikelihoodAndDecadeBigramKey likelihoodAndDecadeBigramKey, DoubleWritable doubleWritable, int i) {
        return likelihoodAndDecadeBigramKey.getDbk().getDecade().hashCode()%i;
    }
}
