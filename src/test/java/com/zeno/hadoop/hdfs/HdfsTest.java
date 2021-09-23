package com.zeno.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.*;

/***
 * 设置环境变量：HADOOP_PROXY_USER=root;HADOOP_USER_NAME=root
 */
@RunWith(JUnit4.class)
public class HdfsTest{

    private Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void setup() throws IOException {
        conf = new Configuration();
        fs = FileSystem.get(conf);
    }

    @Test
    public void mkDir() throws IOException {
        Path mkdir = new Path("/test_mkdir");

        fs.mkdirs(mkdir);
        if (fs.exists(mkdir)){
            fs.delete(mkdir,false);
        }
    }

    @Test
    public void update() throws IOException {
        File helloText = new File("./data/hello.txt");
        FileInputStream is = new FileInputStream(helloText);
        FSDataOutputStream outputStream = fs.create(new Path("/hello1.txt"));
        IOUtils.copyBytes(is,outputStream,conf,true);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

}
