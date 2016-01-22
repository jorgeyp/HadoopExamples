package com.jorgeyp.hadoop.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jorge.yague on 22/01/16.
 */
public class WordReducerTest {
    @Test
    public void returnsWordCount() throws IOException, InterruptedException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new WordCountReducer())
                .withInput(new Text("Lorem"),
                        Arrays.asList(new IntWritable(1), new IntWritable(1)))
                .withOutput(new Text("Lorem"), new IntWritable(2))
                .runTest();
    }
}
