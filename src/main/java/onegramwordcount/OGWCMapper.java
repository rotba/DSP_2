package onegramwordcount;

import common.DecadeBigramKey;
import common.StopWords;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

import static common.Utils.toDecade;

public class OGWCMapper
        extends Mapper<YearOneGram, IntWritable, DecadeBigramKey, IntWritable> {
    private static final Logger logger = Logger.getLogger(OGWCMapper.class);

    public void map(YearOneGram key, IntWritable value, Context context
    ) throws IOException, InterruptedException {
        if(
                !key.isEmpty() &&
                        !StopWords.stopWords.contains(key.getW().toLowerCase())
        ){
            context.write(new DecadeBigramKey(toDecade(key.year), key.w), value);
        }
    }
}
