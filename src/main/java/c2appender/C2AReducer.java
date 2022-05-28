package c2appender;

import common.DecadeBigramKey;
import common.DecadeBigramValue;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class C2AReducer
        extends Reducer<DecadeBigramKey, DecadeBigramValue, DecadeBigramKey, DecadeBigramValue> {
    int lastC2;
    String lastW2;
    private static boolean DEBUG_MODE = true;

    @Override
    protected void setup(Reducer<DecadeBigramKey, DecadeBigramValue, DecadeBigramKey, DecadeBigramValue>.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    public void reduce(DecadeBigramKey key, Iterable<DecadeBigramValue> values,
                       Context context
    ) throws IOException, InterruptedException {
        if(key.getW2().equals(DecadeBigramKey.STAR)){
            reduceOGram(key, values, context);
        }else{
            reduceTGram(key, values, context);
        }
    }

    private void reduceTGram(DecadeBigramKey key, Iterable<DecadeBigramValue> values, Reducer<DecadeBigramKey, DecadeBigramValue, DecadeBigramKey, DecadeBigramValue>.Context context) throws IOException, InterruptedException {
        DecadeBigramValue theVal = null;
        for (DecadeBigramValue val : values) {
            assert theVal!=null;
            theVal = val;
        }
        if(!DEBUG_MODE){
            assert lastW2.equals(key.getW2());
        }
        DecadeBigramValue valueout = new DecadeBigramValue(theVal.c1, lastC2, theVal.c12, theVal.N);
        assert valueout.c1!=-1 && valueout.getC2()!=-1 && valueout.getC12()!=-1 && valueout.getN()!=-1;
        context.write(new DecadeBigramKey(key.getDecade(), key.getW2(), key.getW1()), valueout);
    }

    private void reduceOGram(DecadeBigramKey key, Iterable<DecadeBigramValue> values, Reducer<DecadeBigramKey, DecadeBigramValue, DecadeBigramKey, DecadeBigramValue>.Context context) {
        DecadeBigramValue theVal=null;
        for (DecadeBigramValue val : values) {
            assert theVal==null;
            theVal = val;
        }
        lastC2 = theVal.getC1();
        lastW2 = key.getW2();
    }
}
