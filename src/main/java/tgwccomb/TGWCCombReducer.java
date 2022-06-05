package tgwccomb;

import common.DecadeBigramKey;
import common.DecadeBigramValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TGWCCombReducer
        extends Reducer<DecadeBigramKey, DecadeBigramValue, DecadeBigramKey, DecadeBigramValue> {
    public void reduce(DecadeBigramKey key, Iterable<DecadeBigramValue> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        for (DecadeBigramValue val : values) {
            sum += val.getC12();
        }
        context.write(key, new DecadeBigramValue(-1,-1,sum, -1));
    }
}
