package decadecount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class DCInputFormat extends FileInputFormat<Year, IntWritable> {
    private static final Logger logger = Logger.getLogger(DCMapper.class);

    public DCInputFormat() {
    }

    @Override
    public RecordReader<Year, IntWritable>
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
        logger.log(Level.ERROR, "createRecordReader");
        return new DCRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }

}
