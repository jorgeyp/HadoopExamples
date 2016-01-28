package com.jorgeyp.hadoop.examples.reducesidejoin;

import com.jorgeyp.hadoop.examples.partitioner.totalorderv1.TotalOrderV1Partitioner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by jorge.yague on 28/01/16.
 */
public class ReduceSideJoinDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <input1> <input2> <output>\n");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        deleteOutputFileIfExists(args);

        final Job job = Job.getInstance(getConf());
        job.setJarByClass(this.getClass());
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setPartitionerClass(ReduceSideJoinPartitioner.class);
        job.setGroupingComparatorClass(CompositeKeyWritableGroupingComparator.class);
        job.setReducerClass(ReduceSideJoinReducer.class);

        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TeachersMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DeparmentsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ReduceSideJoinDriver(), args);
        System.exit(exitCode);
    }

    private void deleteOutputFileIfExists(String[] args) throws IOException {
        final Path output = new Path(args[2]);
        FileSystem.get(output.toUri(), getConf()).delete(output, true);
    }
}
