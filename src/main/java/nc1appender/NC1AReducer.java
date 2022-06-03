package nc1appender;

import common.DecadeBigramKey;
import common.DecadeBigramValue;
import common.Props;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

public class NC1AReducer
        extends Reducer<DecadeBigramKey, DecadeBigramValue, DecadeBigramKey, DecadeBigramValue> {
    HashMap<String, Long> decadeTable = new HashMap<>();
    int lastC1;

    @Override
    protected void setup(Reducer<DecadeBigramKey, DecadeBigramValue, DecadeBigramKey, DecadeBigramValue>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        decadeTable = Utils.toMap(context.getConfiguration().get(Main.DECADE_TABLE));
    }

    public void reduce(DecadeBigramKey key, Iterable<DecadeBigramValue> values,
                       Context context
    ) throws IOException, InterruptedException {
        if(Props.DEBUG_MODE) Main.logger.info(String.format("w1: %s, w2: %s", key.getW1(), key.getW2()));
        if(key.getW2().equals(DecadeBigramKey.STAR)){
            reduceOGram(key, values, context);
        }else{
            reduceTGram(key, values, context);
        }
    }

    private void reduceTGram(DecadeBigramKey key, Iterable<DecadeBigramValue> values, Reducer<DecadeBigramKey, DecadeBigramValue, DecadeBigramKey, DecadeBigramValue>.Context context) throws IOException, InterruptedException {
        Long N = decadeTable.get(key.getDecade());
        if(N == null){
            Main.logger.info(String.format("no N for decade %s shouldnt happen", key.getDecade()));
            return;
        }
        int sum = 0;
        for (DecadeBigramValue val : values) {
            sum += val.c12;
        }
        context.write(key, new DecadeBigramValue(lastC1, -1, sum, Math.toIntExact(N)));
    }

    private void reduceOGram(DecadeBigramKey key, Iterable<DecadeBigramValue> values, Reducer<DecadeBigramKey, DecadeBigramValue, DecadeBigramKey, DecadeBigramValue>.Context context) {
        int sum = 0;
        for (DecadeBigramValue val : values) {
            sum += val.c1;
        }
        lastC1 = sum;
    }
}
