package com.zeno.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class TemperaturePerMonthMapper extends Mapper<LongWritable, Text, MapperOutputKey, IntWritable> {

    private static final int SPLIT_SIZE = 4;
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-M-d");


    /** 因为map可能被吊起多次，定义为成员减少gc，同时，你要知道，源码中看到了，
     *  map输出的key，value，是会发生序列化，变成字节数组进入buffer的。
     */
    private MapperOutputKey outputKey = new MapperOutputKey();
    private IntWritable temperature = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] texts = value.toString().split("\\s");
        if (texts.length != SPLIT_SIZE){
            return;
        }

        LocalDate date = LocalDate.parse(texts[0],dateFormatter);

        this.temperature.set(Integer.parseInt(texts[3]));
        outputKey.setValues(date.getYear(),date.getMonthValue(),date.getDayOfMonth(), this.temperature.get());

        context.write(outputKey, temperature);
    }
}