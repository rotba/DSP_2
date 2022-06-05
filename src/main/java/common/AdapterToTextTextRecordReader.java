package common;

import onegramwordcount.OGWCMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

import java.io.IOException;

public class AdapterToTextTextRecordReader extends RecordReader<Text, Text> {
    private static final Logger logger = Logger.getLogger(OGWCMapper.class);

    protected LineRecordReader reader;
    protected Text key;
    protected Text value;

    private static int GRAMS = 1;

    private static int YEAR_IDX = GRAMS;

    public AdapterToTextTextRecordReader() {
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
            key = new Text(reader.getCurrentKey().toString());
            value = reader.getCurrentValue();
            return true;
        } else {
            key = null;
            value = null;
            return false;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }


    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

}
