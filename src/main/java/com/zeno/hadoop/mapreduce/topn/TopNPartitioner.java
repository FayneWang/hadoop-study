package com.zeno.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopNPartitioner extends Partitioner<MapperOutputKey, IntWritable> {

    @Override
    public int getPartition(MapperOutputKey mapperOutputKey, IntWritable intWritable, int numPartitions) {
        return mapperOutputKey.getMonth() % numPartitions;
    }
}
