package common;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class DecadeBigramInputFormat extends FileInputFormat<DecadeBigramKey, DecadeBigramValue> {

    public DecadeBigramInputFormat() {
    }

    @Override
    public RecordReader<DecadeBigramKey, DecadeBigramValue>
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
        return new DecadeBiagramRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }

}
