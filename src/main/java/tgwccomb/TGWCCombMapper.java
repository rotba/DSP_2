package tgwccomb;

import common.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Random;

import static common.Utils.toDecade;

public class TGWCCombMapper
        extends Mapper<Text, Text, DecadeBigramKey, DecadeBigramValue> {
    private static final Logger logger = Logger.getLogger(TGWCCombMapper.class);
    private static int GRAMS = 2;

    private static int YEAR_IDX = GRAMS;
    private YearBiGram key;
    private IntWritable value;
    private int countThresh;
    private int filterPerc;
    private Random rand = new Random();

    @Override
    protected void setup(Mapper<Text, Text, DecadeBigramKey, DecadeBigramValue>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        countThresh = Integer.parseInt(context.getConfiguration().get("A"));
        filterPerc = Integer.parseInt(context.getConfiguration().get("B"));
    }

    public void map(Text _key, Text _value, Context context
    ) throws IOException, InterruptedException {
//        logger.log(Level.ERROR, "map");
        parseKeyValue(_value);
        if (key.isEmpty()) {
            return;
        }
        String cleanW1 = Utils.cleanWord(key.getW1());
        String cleanW2 = Utils.cleanWord(key.getW2());
        if(Utils.filterWord(cleanW1) || Utils.filterWord(cleanW2)){
            return;
        }
        if(filterPerc < rand.nextInt(100)){
            return;
        }
        if (
                !StopWords.stopWords.contains(cleanW1.toLowerCase()) &&
                        !StopWords.stopWords.contains(cleanW2.toLowerCase()) &&
                        value.get() >= countThresh
        ) {
            context.write(new DecadeBigramKey(toDecade(key.year), cleanW1, cleanW2), new DecadeBigramValue(-1, -1, value.get(), -1));
        }
    }

    private void parseKeyValue(Text _value) {
        try {
            if (Props.DEBUG_MODE) logger.info(String.format("line:%s", _value.toString()));
            String[] line = _value.toString().split("\\s+");
            key = new YearBiGram(line[YEAR_IDX], line[0], line[1]);
            value = new IntWritable(Integer.parseInt(line[YEAR_IDX + 1]));
            if (Props.DEBUG_MODE) logger.info(String.format("key:%s, val:%d", key, value.get()));
        } catch (Exception e) {
            key = new YearBiGram(true);
            value = new IntWritable(0);
            logger.error(String.format("couldnt parse line key:%s, exception message:%s", _value.toString(), e.getMessage()));
        } catch (Throwable t) {
            key = new YearBiGram(true);
            value = new IntWritable(0);
            logger.error(String.format("couldnt parse line key:%s, exception message:%s", _value.toString(), t.getMessage()));
        }
    }
}
