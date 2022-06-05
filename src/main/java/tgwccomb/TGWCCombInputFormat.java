package tgwccomb;

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

public class TGWCCombInputFormat extends FileInputFormat<YearBiGram, IntWritable> {
    private static final Logger logger = Logger.getLogger(TGWCCombMapper.class);

    public TGWCCombInputFormat() {
    }

    @Override
    public RecordReader<YearBiGram, IntWritable>
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
        logger.log(Level.ERROR, "createRecordReader");
        return new TGWCCombRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }

}
