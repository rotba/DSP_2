package tgwccomb;

import common.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {
    public static final String BIGRAM_COUNT_MIN_THRESH_KEY = "BIGRAM_COUNT_MIN_THRESH_KEY";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("A", String.valueOf(Props.min2gCountThresh));
        conf.set("B", String.valueOf(Props.percentage));
        Job job = Job.getInstance(conf, "word count histogram");
        job.setJarByClass(Main.class);
        job.setMapperClass(TGWCCombMapper.class);
        job.setReducerClass(TGWCCombReducer.class);
        job.setCombinerClass(TGWCCombReducer.class);
        job.setPartitionerClass(DecadeBigramPartitioner.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputValueClass(DecadeBigramValue.class);
        job.setOutputKeyClass(DecadeBigramKey.class);
        job.setOutputValueClass(DecadeBigramValue.class);
        if(Props.LOCAL){
            job.setInputFormatClass(AdapterToTextTextInputFormat.class);
        }else{
            job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
        }
        job.setOutputFormatClass(TextOutputFormat.class);
        if(Props.LOCAL) job.setNumReduceTasks(3);
        TGWCCombInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}