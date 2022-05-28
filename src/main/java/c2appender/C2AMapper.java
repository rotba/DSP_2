package c2appender;

import common.DecadeBigramKey;
import common.DecadeBigramValue;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class C2AMapper
        extends Mapper<DecadeBigramKey, DecadeBigramValue, DecadeBigramKey, DecadeBigramValue> {
    public static final Logger logger = Logger.getLogger(C2AMapper.class);
    public void map(DecadeBigramKey key, DecadeBigramValue value, Context context
    ) throws IOException, InterruptedException {
        logger.info(key.toString());
        if(key.getW2().equals(DecadeBigramKey.STAR)){
            context.write(key, value);
        }else{
            context.write(new DecadeBigramKey(key.getDecade(), key.getW2(), key.getW1()), value);
        }

    }
}
