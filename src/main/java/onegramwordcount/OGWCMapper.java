package onegramwordcount;

import common.DecadeBiGram;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

import static common.Utils.toDecade;

public class OGWCMapper
        extends Mapper<YearOneGram, IntWritable, DecadeBiGram, IntWritable> {
    private static final Logger logger = Logger.getLogger(OGWCMapper.class);

    public void map(YearOneGram key, IntWritable value, Context context
    ) throws IOException, InterruptedException {
        logger.log(Level.ERROR, "map");
        context.write(new DecadeBiGram(toDecade(key.year), key.w, false), value);
    }
}
