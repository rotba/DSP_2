package twogramworccount;

import common.Props;
import onegramwordcount.YearOneGram;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TGWCRecordReader extends RecordReader<YearBiGram, IntWritable> {
    private static final Logger logger = Logger.getLogger(TGWCMapper.class);

    protected LineRecordReader reader;
    protected YearBiGram key;
    protected IntWritable value;

    private static int GRAMS = 2;

    private static int YEAR_IDX = GRAMS;

    public TGWCRecordReader() {
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
                key = new YearBiGram(line[YEAR_IDX], line[0], line[1]);
                value = new IntWritable(Integer.parseInt(line[YEAR_IDX+1]));
                if(Props.DEBUG_MODE)logger.info(String.format("key:%s, val:%d", key, value.get()));
            }catch (Exception e){
                key = new YearBiGram(true);
                value = new IntWritable(0);
                logger.error(String.format("couldnt parse line key:%s, exception message:%s", reader.getCurrentValue(), e.getMessage()));
            }catch (Throwable t){
                key = new YearBiGram(true);
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
    public YearBiGram getCurrentKey() throws IOException, InterruptedException {
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
