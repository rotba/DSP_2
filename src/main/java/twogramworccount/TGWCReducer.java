package twogramworccount;

import common.DecadeBigramKey;
import common.DecadeBigramValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TGWCReducer
        extends Reducer<DecadeBigramKey, IntWritable, DecadeBigramKey, DecadeBigramValue> {
    public void reduce(DecadeBigramKey key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new DecadeBigramValue(-1,-1,sum, -1));
    }
}
