package com.zeno.hadoop.mapreduce.topn;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class TopN {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        String []other = new GenericOptionsParser(conf,args).getRemainingArgs();
        conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf,"TopN");
        job.setJarByClass(TopN.class);

        // input
        TextInputFormat.addInputPath(job,new Path("data/topN"));

        // output
        Path outPath = new Path("output");
        FileSystem fileSystem = outPath.getFileSystem(conf);
        if (fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }
        TextOutputFormat.setOutputPath(job,outPath);

        // map
        job.setMapperClass(TemperaturePerMonthMapper.class);

        // key
        job.setMapOutputKeyClass(MapperOutputKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        // partitioner
        job.setPartitionerClass(TopNPartitioner.class);

        // sort 年，月，温度 且温度倒序
        job.setSortComparatorClass(TopNComparator.class);

        // combine

        // reducetask

        // groupingComparator
        job.setGroupingComparatorClass(TopNGroupingComparator.class);

        // reduce
        job.setReducerClass(TemperaturePerMonthReducer.class);

        job.waitForCompletion(true);

    }
}
