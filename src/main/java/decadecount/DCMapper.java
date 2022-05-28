package decadecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

import static common.Utils.toDecade;

public class DCMapper
        extends Mapper<Year, IntWritable, Text, IntWritable> {
    private static final Logger logger = Logger.getLogger(DCMapper.class);

    public void map(Year key, IntWritable value, Context context
    ) throws IOException, InterruptedException {
        context.write(new Text(toDecade(key.year)), value);
    }
}
