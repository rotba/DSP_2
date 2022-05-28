package onegramwordcount;

import common.DecadeBiGram;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count histogram");
        job.setJarByClass(Main.class);
        job.setMapperClass(OGWCMapper.class);
        job.setCombinerClass(OGWCReducer.class);
        job.setReducerClass(OGWCReducer.class);
        job.setOutputKeyClass(DecadeBiGram.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(OGWCInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        OGWCInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}