package com.jorgeyp.hadoop.examples.writables.aggregation2d;

import com.jorgeyp.hadoop.examples.writables.PointWritable;
import es.afm.hadoop.examples.writables.counters.Counters;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by jorge.yague on 25/01/16.
 */
public class Aggregation2dDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job(getConf(), "Aggregation 2D");
        job.setJarByClass(getClass());
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Aggregation2dMapper.class);
        job.setReducerClass(Aggregation2dReducer.class);

        job.setMapOutputKeyClass(PointWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(PointWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        System.out.println("# Malformed records: "
                + job.getCounters().findCounter(Counters.MALFORMED_RECORD)
                .getValue());
        System.out.println("# Wrong values in records records: "
                + job.getCounters().findCounter(Counters.WRONG_VALUE)
                .getValue());


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Aggregation2dDriver(), args);
        System.exit(exitCode);
    }
}
