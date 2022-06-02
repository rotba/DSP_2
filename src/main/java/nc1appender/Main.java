package nc1appender;

import common.DecadeBigramKey;
import common.DecadeBigramInputFormat;
import common.DecadeBigramValue;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Main {
    public static final String DECADE_TABLE = "decade_table";
    public static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        //the second boolean parameter here sets the recursion to true
        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(
                new Path(args[0]), true);
        String decadeTable = "";
        while(fileStatusListIterator.hasNext()){
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            Path path = fileStatus.getPath();
            if (path.getName().contains("part")){
                FSDataInputStream inputStream = fs.open(path);
                decadeTable += IOUtils.toString(inputStream, "UTF-8");
            }
        }
        conf.set(DECADE_TABLE, decadeTable);
        conf.reloadConfiguration();
        fs.close();
        Job job = Job.getInstance(conf, "word count histogram");
        job.setJarByClass(Main.class);
        job.setReducerClass(NC1AReducer.class);
        job.setOutputKeyClass(DecadeBigramKey.class);
        job.setOutputValueClass(DecadeBigramValue.class);
        job.setInputFormatClass(DecadeBigramInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        logger.log(Level.INFO, conf.get(DECADE_TABLE));
        DecadeBigramInputFormat.addInputPath(job, new Path(args[1]));
        DecadeBigramInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}