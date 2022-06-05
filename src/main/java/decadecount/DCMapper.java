package decadecount;

import common.StopWords;
import onegramwordcount.YearOneGram;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

import static common.Utils.toDecade;

public class DCMapper
        extends Mapper<Text, Text, Text, IntWritable> {
    private static final Logger logger = Logger.getLogger(DCMapper.class);
    private Year key;
    private IntWritable value;
    private static int GRAMS = 1;

    private static int YEAR_IDX = GRAMS;
    public void map(Text _key, Text _value, Context context
    ) throws IOException, InterruptedException {
        parseKeyValue(_value);
        if(value.get() >0){
            context.write(new Text(toDecade(key.year)), value);
        }
    }

    private void parseKeyValue(Text _value) {
        String[] line = _value.toString().split("\\s+");
        key = new Year(line[YEAR_IDX]);
        if(StopWords.stopWords.contains(line[0].toLowerCase())){
            value = new IntWritable(0);
        }else{
            value = new IntWritable(Integer.parseInt(line[YEAR_IDX+1]));
        }
    }
}
