package com.zeno.hadoop.mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 用于reduce分组
 */
public class TopNGroupingComparator extends WritableComparator {

    public TopNGroupingComparator(){
        super(MapperOutputKey.class,true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MapperOutputKey outputKey1 = (MapperOutputKey)a;
        MapperOutputKey outputKey2 = (MapperOutputKey)b;


        int yearComp = Integer.compare(outputKey1.getYear(),outputKey2.getYear());
        if (yearComp != 0){
            return yearComp;
        }

        return Integer.compare(outputKey1.getMonth(),outputKey2.getMonth());
    }
}
