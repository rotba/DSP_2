package tgwccomb;

import common.DecadeBigramKey;
import common.DecadeBigramValue;
import common.Props;
import common.StopWords;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

import static common.Utils.toDecade;

public class TGWCCombMapper
        extends Mapper<Text, Text, DecadeBigramKey, DecadeBigramValue> {
    private static final Logger logger = Logger.getLogger(TGWCCombMapper.class);
    private static int GRAMS = 2;

    private static int YEAR_IDX = GRAMS;
    private YearBiGram key;
    private IntWritable value;

    public void map(Text _key, Text _value, Context context
    ) throws IOException, InterruptedException {
//        logger.log(Level.ERROR, "map");
        parseKeyValue(_value);
        if (
                !key.isEmpty() &&
                        !StopWords.stopWords.contains(key.getW1().toLowerCase()) &&
                        !StopWords.stopWords.contains(key.getW2().toLowerCase())
        ) {
            context.write(new DecadeBigramKey(toDecade(key.year), key.w1, key.w2), new DecadeBigramValue(-1,-1,value.get(),-1));
        }
    }

    private void parseKeyValue(Text _value) {
        try{
            if(Props.DEBUG_MODE)logger.info(String.format("line:%s", _value.toString()));
            String[] line = _value.toString().split("\\s+");
            key = new YearBiGram(line[YEAR_IDX], line[0], line[1]);
            value = new IntWritable(Integer.parseInt(line[YEAR_IDX+1]));
            if(Props.DEBUG_MODE)logger.info(String.format("key:%s, val:%d", key, value.get()));
        }catch (Exception e){
            key = new YearBiGram(true);
            value = new IntWritable(0);
            logger.error(String.format("couldnt parse line key:%s, exception message:%s", _value.toString(), e.getMessage()));
        }catch (Throwable t){
            key = new YearBiGram(true);
            value = new IntWritable(0);
            logger.error(String.format("couldnt parse line key:%s, exception message:%s", _value.toString(), t.getMessage()));
        }
    }
}
