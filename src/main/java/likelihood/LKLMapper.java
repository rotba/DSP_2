package likelihood;

import common.DecadeBigramKey;
import common.DecadeBigramValue;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.mahout.math.stats.LogLikelihood;

import java.io.IOException;

public class LKLMapper
        extends Mapper<DecadeBigramKey, DecadeBigramValue, LikelihoodAndDecadeBigramKey, DoubleWritable> {
    public static final Logger logger = Logger.getLogger(LKLMapper.class);
    public void map(DecadeBigramKey key, DecadeBigramValue value, Context context
    ) throws IOException, InterruptedException {
        double likelihood = calcLikelyHood(value.getC1(), value.getC2(),value.getC12(), value.getN());
        context.write(new LikelihoodAndDecadeBigramKey(likelihood, key), new DoubleWritable(likelihood));
    }

    private double calcLikelyHood(int c1, int c2, int c12, int n) {
        return LogLikelihood.logLikelihoodRatio(c12, c1, c2, n);
    }
}
