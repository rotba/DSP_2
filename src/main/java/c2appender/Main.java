package c2appender;

import common.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

public class Main {
    public static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        logger.info(String.format("%s %s %s", args[0], args[1], args[2]));
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count histogram");
        job.setJarByClass(Main.class);
        job.setMapperClass(C2AMapper.class);
        job.setReducerClass(C2AReducer.class);
        job.setPartitionerClass(DecadeBigramPartitioner.class);
        job.setOutputKeyClass(DecadeBigramKey.class);
        job.setOutputValueClass(DecadeBigramValue.class);
        job.setInputFormatClass(DecadeBigramInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        if(Props.LOCAL) job.setNumReduceTasks(3);
        DecadeBigramInputFormat.addInputPath(job, new Path(args[0]));
        DecadeBigramInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}