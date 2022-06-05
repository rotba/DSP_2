package common;

import onegramwordcount.OGWCMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

public class AdapterToTextTextInputFormat extends FileInputFormat<Text, Text> {
    private static final Logger logger = Logger.getLogger(OGWCMapper.class);

    public AdapterToTextTextInputFormat() {
    }

    @Override
    public RecordReader<Text, Text>
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
        return new AdapterToTextTextRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }

}
