package com.celinelay.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by celinelay on 11/10/2016.
 */
public class ProportionMaleFemale {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private Text word = new Text();
        private Text word2 = new Text();
        private Text word3 = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            String[] words = value.toString().split(";");

            if(words[1].contains("m") && !words[1].contains("f")){
                word.set("m");
                word2.set("f");
                word3.set("f, m");
            }
            else if (words[1].contains("f") && !words[1].contains("m")){
                word.set("f");
                word2.set("m");
                word3.set("f, m");
            }
            else{
                word.set("f, m");
                word2.set("m");
                word3.set("f");
            }
            context.write(word, one);
            context.write(word2, zero);
            context.write(word3, zero);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }

            double res = (double) sum / count;
            result.set(res);
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Counting proportion of male or female names");
        job.setJarByClass(ProportionMaleFemale.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
