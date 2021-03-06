package com.jorgeyp.hadoop.examples.partitioner.totalorderv2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by jorge.yague on 25/01/16.
 */
public class TotalOrderV2Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        deleteOutputFileIfExists(args);

        final Job job = Job.getInstance(getConf());
        // local mode execution will turn this to 1
        job.setNumReduceTasks(2);

        job.setJarByClass(TotalOrderV2Driver.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setPartitionerClass(TotalOrderPartitioner.class);
        InputSampler.RandomSampler<LongWritable, Text> sampler = new InputSampler.RandomSampler<LongWritable, Text>(0.1, 15);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(args[1] + "/_partition.lst"));
        InputSampler.writePartitionFile(job, sampler);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TotalOrderV2Driver(), args);
        System.exit(exitCode);
    }

    private void deleteOutputFileIfExists(String[] args) throws IOException {
        final Path output = new Path(args[1]);
        FileSystem.get(output.toUri(), getConf()).delete(output, true);
    }
}
