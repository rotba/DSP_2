package common;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

import java.io.IOException;

public class DecadeBiagramRecordReader extends RecordReader<DecadeBigramKey, DecadeBigramValue> {

    protected LineRecordReader reader;
    protected DecadeBigramKey key;
    protected DecadeBigramValue value;
    public static final Logger logger = Logger.getLogger(DecadeBiagramRecordReader.class);

    public DecadeBiagramRecordReader() {
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
            String[] line = reader.getCurrentValue().toString().split("\\s+");
            if(line.length<3){
                logger.error("XXXXXXXXXXXXXXXXXXXXXXXXYYYYYYYYYYYYYYYY" + reader.getCurrentValue());
                key = new DecadeBigramKey(false);
                value = new DecadeBigramValue();
                return true;
            }
            key = new DecadeBigramKey(line[0], line[1], line[2]);
            value = new DecadeBigramValue(
                    Integer.parseInt(line[3]),
                    Integer.parseInt(line[4]),
                    Integer.parseInt(line[5]),
                    Integer.parseInt(line[6]));
            if(Props.DEBUG_MODE) logger.info(String.format("key: %s, val: %s", key.toString(), value.toString()));
            return true;
        } else {
            key = null;
            value = null;
            return false;
        }
    }

    @Override
    public DecadeBigramKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public DecadeBigramValue getCurrentValue() throws IOException, InterruptedException {
        return value;
    }


    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

}
