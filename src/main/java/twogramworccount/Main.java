package twogramworccount;

import common.DecadeBiGram;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count histogram");
        job.setJarByClass(Main.class);
        job.setMapperClass(TGWCMapper.class);
        job.setCombinerClass(TGWCReducer.class);
        job.setReducerClass(TGWCReducer.class);
        job.setOutputKeyClass(DecadeBiGram.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TGWCInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TGWCInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}