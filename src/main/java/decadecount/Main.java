package decadecount;

import common.AdapterToTextTextInputFormat;
import common.DecadeBigramPartitioner;
import common.Props;
import onegramwordcount.OGWCInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count histogram");
        job.setJarByClass(Main.class);
        job.setMapperClass(DCMapper.class);
        job.setCombinerClass(DCReducer.class);
        job.setReducerClass(DCReducer.class);
        job.setPartitionerClass(DecadeCountPrtitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        if(Props.LOCAL){
            job.setInputFormatClass(AdapterToTextTextInputFormat.class);
        }else{
            job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
        }
        job.setOutputFormatClass(TextOutputFormat.class);
        if(Props.LOCAL) job.setNumReduceTasks(3);
        DCInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}