package onegramwordcount;

import common.DecadeBigramKey;
import common.Props;
import common.StopWords;
import common.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

import static common.Utils.toDecade;

public class OGWCMapper
        extends Mapper<Text, Text, DecadeBigramKey, IntWritable> {
    private static final Logger logger = Logger.getLogger(OGWCMapper.class);
    private static int GRAMS = 1;

    private static int YEAR_IDX = GRAMS;
    private YearOneGram key;
    private IntWritable value;

    public void map(Text _key, Text _value, Context context
    ) throws IOException, InterruptedException {
        if (Props.DEBUG_MODE)
            logger.info(String.format("XXXXXXXXXXXXXXX key:%s, value:%s", _key.toString(), _value.toString()));
        parseKeyValue(_value);
        if (key.isEmpty()) {
            return;
        }
        String cleanW = Utils.cleanWord(key.getW());
        if(Utils.filterWord(cleanW)){
            return;
        }
        if (
                !StopWords.stopWords.contains(cleanW.toLowerCase())
        ) {
            context.write(new DecadeBigramKey(toDecade(key.year), cleanW), value);
        }
    }

    private void parseKeyValue(Text _value) {
        try {
            if (Props.DEBUG_MODE) logger.info(String.format("line:%s", _value.toString()));
            String[] line = _value.toString().split("\\s+");
            key = new YearOneGram(line[YEAR_IDX], line[0]);
            value = new IntWritable(Integer.parseInt(line[YEAR_IDX + 1]));
            if (Props.DEBUG_MODE) logger.info(String.format("key:%s, val:%d", key, value.get()));
        } catch (Exception e) {
            key = new YearOneGram(true);
            value = new IntWritable(0);
            logger.error(String.format("couldnt parse line key:%s, exception message:%s", _value.toString(), e.getMessage()));
        } catch (Throwable t) {
            key = new YearOneGram(true);
            value = new IntWritable(0);
            logger.error(String.format("couldnt parse line key:%s, exception message:%s", _value.toString(), t.getMessage()));
        }
    }
}
