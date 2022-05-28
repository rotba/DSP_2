package twogramworccount;

import common.DecadeBigramKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

import static common.Utils.toDecade;

public class TGWCMapper
        extends Mapper<YearBiGram, IntWritable, DecadeBigramKey, IntWritable> {
    private static final Logger logger = Logger.getLogger(TGWCMapper.class);

    public void map(YearBiGram key, IntWritable value, Context context
    ) throws IOException, InterruptedException {
//        logger.log(Level.ERROR, "map");
        context.write(new DecadeBigramKey(toDecade(key.year), key.w1,key.w2), value);
    }
}
