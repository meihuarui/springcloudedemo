package com.atguigu.hdfs;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.net.URI;

public class HDFSClient {

    private static final String HDFS_URL="hdfs://hadoop102:9000";
    private static final String LOCAL_USER="atguigu";

    @Test
    public void testListStatus() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), new Configuration(), LOCAL_USER);
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {

            boolean directory = fileStatus.isDirectory();
            if (directory){
                System.out.println(fileStatus.getPath().getName() + " 是文件夹");
            }else {
                System.out.println(fileStatus.getPath().getName()+" 是文件");
            }
        }
    }

    @Test
    public void testListFiles() throws Exception{
        FileSystem fs = FileSystem.get(new URI(HDFS_URL), new Configuration(), LOCAL_USER);
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while (files.hasNext()){
            System.out.println(files.next().getPath());
            System.out.println(files.next().getGroup());
            System.out.println(files.next().getOwner());
            System.out.println(files.next().getAccessTime());
            BlockLocation[] blockLocations = files.next().getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {

                    System.out.println("     :     "+host);
                }
            }
            System.out.println("==============");
            /* ----------------  -------------- */

        }
        System.out.println("heheheheheh");
    }

    @Test
    public void testRename() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), new Configuration(), LOCAL_USER);
        boolean rename = fileSystem.rename(new Path("/wcy.txt"), new Path("/weichaoyang.txt"));
        fileSystem.close();
        System.out.println("hello3");
    }

    @Test
    public void testDelete() throws Exception{
        FileSystem fs = FileSystem.get(new URI(HDFS_URL), new Configuration(), LOCAL_USER);
        fs.delete(new Path("/in.txt"),true);
        fs.close();
        System.out.println("hello ");
    }

    @Test
    public void testCopyToLocalFile() throws Exception{
        FileSystem fs = FileSystem.get(new URI(HDFS_URL), new Configuration(), LOCAL_USER);
        fs.copyToLocalFile(false,new Path("/in.txt"),new Path("d://wcyin.txt"),true);
        fs.close();
        System.out.println("over2");

    }
    public static void main(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "atguigu");
        fs.mkdirs(new Path("/wife/maruifang/meihuarui"));
        fs.close();
        System.out.println("哈哈");
    }

    @Test
    public void testCopyFromLocalFile() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "atguigu");
        fileSystem.copyFromLocalFile(new Path("d://wcy.txt"),new Path("/wcy.txt"));
        fileSystem.close();
        System.out.println("over");

    }

    @Test
    public void test() throws Exception{
        Configuration entries = new Configuration();
        entries.set("dfs.replication","2");
        FileSystem atguigu = FileSystem.get(new URI("hdfs://hadoop102:9000"), entries, "atguigu");
        atguigu.copyFromLocalFile(new Path("d:/wcy.txt"),new Path("/mrf.txt"));
        atguigu.close();
        System.out.println("over");
    }
}
