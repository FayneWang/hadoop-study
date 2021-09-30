package com.zeno.hadoop.mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 用于Mapper输出排离
 */
public class TopNComparator extends WritableComparator {

    public TopNComparator(){
        super(MapperOutputKey.class,true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MapperOutputKey outputKey1 = (MapperOutputKey)a;
        MapperOutputKey outputKey2 = (MapperOutputKey)b;

        //我们为了让这个案例体现api开发，所以下边的逻辑是一种通用的逻辑：按照时间正序，
        //但是我们目前业务需要的是  年，月，温度，且温度倒序，所以一会还得开发一个sortComparator。。。。
        //the value 0 if x == y; a value less than 0 if x < y; and a value greater than 0 if x > y

        int yearComp = Integer.compare(outputKey1.getYear(),outputKey2.getYear());
        if (yearComp != 0){
            return yearComp;
        }

        int monthComp = Integer.compare(outputKey1.getMonth(),outputKey2.getMonth());
        if (monthComp != 0){
            return monthComp;
        }

        return -Integer.compare(outputKey1.getTemperature(),outputKey2.getTemperature());
    }
}
