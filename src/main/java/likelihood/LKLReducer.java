package likelihood;

import common.DecadeBigramKey;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LKLReducer
        extends Reducer<LikelihoodAndDecadeBigramKey, DoubleWritable, DecadeBigramKey, DoubleWritable> {
    int currDecadeCount;
    String lastDecade = null;
    private static boolean DEBUG_MODE = true;
    private static final int MAX = 100;


    public void reduce(LikelihoodAndDecadeBigramKey key, Iterable<DoubleWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        if(key.getDbk().getDecade().equals(lastDecade)){
           if(currDecadeCount == MAX){
               return;
           }else{
               currDecadeCount++;
           }
        }else{
            lastDecade = key.getDbk().getDecade();
            currDecadeCount = 0;
        }
        DoubleWritable theVal = null;
        for (DoubleWritable val :
                values) {
            assert theVal != null;
            theVal = val;
        }
        context.write(key.getDbk(), theVal);
    }
}
