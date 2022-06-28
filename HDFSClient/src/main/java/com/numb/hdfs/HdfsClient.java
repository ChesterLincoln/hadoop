package com.numb.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * 客户端代码常用套路：
 * 1. 获取一个客户端对象
 * 2. 执行相关的操作命令
 * 3. 关闭资源
 * 常用：HDFS ZOOKEEPER
 * <p>
 * 参数优先级：（从低到高）
 * hdfs-default.xml -> hdfs-site.xml -> 在项目资源目录下的配制文件 ->代码里的配制Configuration
 * 就近原则
 *
 * @author gaoxiangyu
 */
public class HdfsClient {

    private FileSystem fs;

    /**
     * 初始化文件系统
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {

        Configuration configuration = new Configuration();

        configuration.set("dfs.replication", "2");

        URI uri = new URI("hdfs://hadoop1:8020");

        String user = "hadoop";

        fs = FileSystem.get(uri, configuration, user);

    }

    /**
     * 关闭文件系统
     *
     * @throws IOException
     */
    @After
    public void close() throws IOException {

        fs.close();

    }

    /**
     * 创建路径
     *
     * @throws IOException
     */
    @Test
    public void testMkdirs() throws IOException {

        // 参数解读
        // 参数一：f 目的地路径

        Path f = new Path("/xiyou/huaguoshan/");
        fs.mkdirs(f);

    }

    /**
     * 上传文件
     *
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {

        // 参数解读
        // 参数一：delSrc 表示删除原数据（本地即将上传的数据）
        // 参数二：overwrite 表示是否覆盖
        // 参数三：src 原数据路径
        // 参数四：dst 目的地路径

        Path src = new Path("./src/main/resources/sunwukong");
        Path dst = new Path("hdfs://hadoop1/xiyou/huaguoshan/");
        fs.copyFromLocalFile(false, false, src, dst);
    }

    /**
     * 下载文件
     *
     * @throws IOException
     */
    @Test
    public void testGet() throws IOException {

        // 参数解读
        // 参数一：delSrc 表示删除原数据（服务器即将下载的数据）
        // 参数二：src 元数据路径
        // 参数三：dst 目的地路径
        // 参数四：useRawLocalFileSystem 是否开启本地校验 (crc文件是循环冗余码校验)

        Path src = new Path("hdfs://hadoop1/xiyou/huaguoshan/sunwukong.txt");
        Path dst = new Path("./src/main/resources/download/");
        fs.copyToLocalFile(false, src, dst, false);
    }

    /**
     * 删除文件
     * 可以删除文件 空目录 非空目录 注意参数递归删除即可
     *
     * @throws IOException
     */
    @Test
    public void testRm() throws IOException {

        // 参数解读
        // 参数一：f 要删除的路径
        // 参数二：recursive 是否递归

        Path src = new Path("hdfs://hadoop1/xiyou/huaguoshan/sunwukong.txt");
        fs.delete(src, false);
    }

    /**
     * 文件或目录的重命名和移动
     * 本质上同linux的mv命令一致
     *
     * @throws IOException
     */
    @Test
    public void testMove() throws IOException {

        // 参数解读
        // 参数一：src 源文件路径
        // 参数二：dst 目标文件路径

        Path src = new Path("hdfs://hadoop1/xiyou/huaguoshan/sunwukong.txt");
        Path dst = new Path("hdfs://hadoop1/xiyou/huaguoshan/sunwukong-new.txt");
        fs.rename(src, dst);
    }

    /**
     * 获取文件详细信息
     *
     * @throws IOException
     */
    @Test
    public void testFileDetail() throws IOException {

        // 参数解读
        // 参数一：f 文件路径
        // 参数二：recursive 是否递归

        // 获取所有文件信息
        Path f = new Path("hdfs://hadoop1/");
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(f, true);

        // 遍历迭代器
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();

            System.out.println("=====" + fileStatus.getPath() + "=====");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            // 块信息里包含块起止 和 存储节点信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    /**
     * 判断是文件还是文件夹
     *
     * @throws IOException
     */
    @Test
    public void testFile() throws IOException {

        Path f = new Path("hdfs://hadoop1/");
        FileStatus[] listStatus = fs.listStatus(f);
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println("file: " + status.getPath().getName());
            } else {
                System.out.println("dir: " + status.getPath().getName());
            }
        }
    }
}
