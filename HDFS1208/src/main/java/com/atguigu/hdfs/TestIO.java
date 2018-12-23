package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

public class TestIO {
    @Test
    public void readFileSeek1() throws Exception {
        Configuration entries = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), entries, "atguigu");
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        FileOutputStream fos = new FileOutputStream("d://hadoop-2.7.2.part1");
        byte[] bytes = new byte[1024];
        for (int i = 0; i < 1024*128; i++) {
            fis.read(bytes);
            fos.write(bytes);
        }
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    @Test
    public void readFileSeek2() throws Exception {
        Configuration entries = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), entries, "atguigu");
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        fis.seek(1024*1024*128);
        FileOutputStream fos = new FileOutputStream("d://hadoop-2.7.2.part2");
        IOUtils.copyBytes(fis,fos,entries);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();

    }

    @Test
    public void test() throws Exception {
        FileSystem atguigu = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "atguigu");
        FSDataInputStream inputStream = atguigu.open(new Path("/pjl.txt"));
        FileOutputStream fileOutputStream = new FileOutputStream("d://wwccyy.txt");
        IOUtils.copyBytes(inputStream, fileOutputStream, new Configuration(), true);
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(inputStream);
        System.out.println("呵呵呵额 ");

    }

    @Test
    public void putFileToHDFS() throws Exception {
        FileInputStream in = new FileInputStream("d:/wcy.txt");
        Configuration entries = new Configuration();
        FileSystem atguigu = FileSystem.get(new URI("hdfs://hadoop102:9000"), entries, "atguigu");
        FSDataOutputStream outputStream = atguigu.create(new Path("/wcy.txt"));
        IOUtils.copyBytes(in, outputStream, entries);
        IOUtils.closeStream(outputStream);
        in.close();
    }
}
