package com.faeterj.sbd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App {

    public static class TokenizerMapper extends
            Mapper<Object,   /*Input key Type*/
                Text,        /*Input value Type*/
                Text,        /*Output key Type*/
                IntWritable> /*Output value Type*/
    {


        // Map ...
        public void map(Object key, Text value,  Context context) throws IOException {

            IntWritable one = new IntWritable(1);
            String valueString = value.toString();
            String[] SingleCountryData = valueString.split(",");
            Text word = new Text(SingleCountryData[7]);

            try {
                context.write(word, one);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        // Reduce ...
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int frequencyForCountry = 0;

            for (IntWritable val : values) {
                int v = val.get();
                frequencyForCountry += v;
            }

            context.write(key, new IntWritable(frequencyForCountry));
        }
    }


    public static class DescendingKeyComparator extends WritableComparator {
        protected DescendingKeyComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Text key1 = (Text) w1;
            Text key2 = (Text) w2;
            return -1 * key1.compareTo(key2);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "sdb");
        job.setJarByClass(App.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // job.setSortComparatorClass(DescendingKeyComparator.class);
        
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // deleting the output path automatically
        output.getFileSystem(conf).delete(output, true);

        // exiting the job only if the flag value becomes false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}