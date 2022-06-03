package nc1appender;

import common.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;

public class Main {
    public static final String DECADE_TABLE = "decade_table";
    public static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String decadeTable = "";
        for (int i = 0; i < 100; i++) {
            int digitsToCut = i==0 ? 1 : (int)Math.log10(i)+1;
            String pathS = args[0].substring(0, args[0].length() - digitsToCut)+i;
            logger.info(String.format("pathS: %s", pathS));
            Path path = new Path(pathS);
            FileSystem fs = path.getFileSystem(conf);
            try{
                FSDataInputStream inputStream = fs.open(path);
                String chunk = IOUtils.toString(inputStream, "UTF-8");
                logger.info(String.format("decades table chunk: %s", chunk));
                decadeTable += chunk;
                fs.close();
            }catch (FileNotFoundException e){
                break;
            }finally{
                fs.close();
            }
        }
        conf.set(DECADE_TABLE, decadeTable);
        conf.reloadConfiguration();
        Job job = Job.getInstance(conf, "word count histogram");
        job.setJarByClass(Main.class);
        job.setReducerClass(NC1AReducer.class);
        job.setPartitionerClass(DecadeBigramPartitioner.class);
        job.setOutputKeyClass(DecadeBigramKey.class);
        job.setOutputValueClass(DecadeBigramValue.class);
        job.setInputFormatClass(DecadeBigramInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        if(Props.LOCAL) job.setNumReduceTasks(3);
        logger.log(Level.INFO, conf.get(DECADE_TABLE));
        DecadeBigramInputFormat.addInputPath(job, new Path(args[1]));
        DecadeBigramInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}