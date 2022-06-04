package onegramwordcount;

import common.Props;
import nc1appender.Main;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

public class OGWCRecordReader extends RecordReader<YearOneGram, IntWritable> {
    private static final Logger logger = Logger.getLogger(OGWCMapper.class);

    protected LineRecordReader reader;
    protected YearOneGram key;
    protected IntWritable value;

    private static int GRAMS = 1;

    private static int YEAR_IDX = GRAMS;

    public OGWCRecordReader() {
        reader = new LineRecordReader();
        key = null;
        value = null;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        reader.initialize(split, context);
    }


    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (reader.nextKeyValue()) {
            try{
                if(Props.DEBUG_MODE)logger.info(String.format("line:%s", reader.getCurrentValue()));
                String[] line = reader.getCurrentValue().toString().split("\\s+");
                key = new YearOneGram(line[YEAR_IDX], line[0]);
                value = new IntWritable(Integer.parseInt(line[YEAR_IDX+1]));
                if(Props.DEBUG_MODE)logger.info(String.format("key:%s, val:%d", key, value.get()));
            }catch (Exception e){
                key = new YearOneGram(true);
                value = new IntWritable(0);
                logger.error(String.format("couldnt parse line key:%s, exception message:%s", reader.getCurrentValue(), e.getMessage()));
            }catch (Throwable t){
                key = new YearOneGram(true);
                value = new IntWritable(0);
                logger.error(String.format("couldnt parse line key:%s, exception message:%s", reader.getCurrentValue(), t.getMessage()));
            }
            return true;
        } else {
            key = null;
            value = null;
            return false;
        }
    }

    @Override
    public YearOneGram getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public IntWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }


    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

}
