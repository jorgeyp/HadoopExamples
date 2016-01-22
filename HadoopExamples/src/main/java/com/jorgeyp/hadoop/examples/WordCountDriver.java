package com.jorgeyp.hadoop.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by jorge.yague on 22/01/16.
 */
public class WordCountDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job wordCountJob = new Job(getConf(), "Word count");
        wordCountJob.setJarByClass(getClass());

        FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));

        wordCountJob.setMapperClass(WordCountMapper.class);
        wordCountJob.setCombinerClass(WordCountReducer.class);
        wordCountJob.setReducerClass(WordCountReducer.class);

        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);

        wordCountJob.waitForCompletion(true);

        return wordCountJob.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountDriver(), args);
        System.exit(exitCode);
    }
}
