package com.jorgeyp.hadoop.examples.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jorge.yague on 22/01/16.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().trim().split("\\s+");

        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
