package likelihood;

import common.DecadeBigramInputFormat;
import common.DecadeBigramKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

public class Main {
    public static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count histogram");
        job.setJarByClass(Main.class);
        job.setMapperClass(LKLMapper.class);
        job.setReducerClass(LKLReducer.class);
        job.setPartitionerClass(LKLPartitioner.class);
        job.setMapOutputKeyClass(LikelihoodAndDecadeBigramKey.class);
        job.setOutputKeyClass(DecadeBigramKey.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(DecadeBigramInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        DecadeBigramInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}