
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;


import java.io.IOException;
import java.net.URI;

/**
 * Created by yyq on 11/20/15.
 */


public class HDFSDemo {
    //HDFS访问地址
    private static final String HDFS = "hdfs://localhost:9000/user/yyq";
    //hdfs路径
    private String hdfsPath;
    //Hadoop系统配置
    private Configuration conf;


    public HDFSDemo(Configuration conf) {
        this(HDFS, conf);
    }

    public HDFSDemo (String hdfs, Configuration conf) {
        this.hdfsPath = hdfs;
        this.conf = conf;
    }

    //加载Hadoop配置文件
    public  static JobConf setConfig(){
        JobConf conf = new JobConf(HDFSDemo .class);
        conf.setJobName("HDFSDemo");
        conf.addResource("classpath:/hadoop/hadoop-1.0.3/conf/core-site.xml");
        conf.addResource("classpath:/hadoop/hadoop-1.0.3/conf/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/hadoop-1.0.3/conf/mapred-site.xml");
        return conf;
    }

    //在根目录下创建文件夹
    public void mkdirs(String folder) throws IOException {
        Path path = new Path(folder);
        System.out.println(URI.create(hdfsPath));
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }else{
            System.out.println(path+" is Exists");
        }
        fs.close();
    }

    //某个文件夹的文件列表
    public FileStatus[] ls(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FileStatus[] list = fs.listStatus(path);
        System.out.println("ls: " + folder);
        System.out.println("==========================================================");
        if(list != null){
            for (FileStatus f : list) {
                System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDir(), f.getLen());
                System.out.printf("%s, folder: %s, 大小: %dK\n", f.getPath().getName(), (f.isDir()?"目录":"文件"), f.getLen()/1024);
            }
        }
        System.out.println("==========================================================");
        fs.close();
        return  list;
    }

    //复制文件或文件夹
    public void copyFile(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        //remote---/用户/用户下的文件或文件夹
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }

    //递归删除文件或文件夹
    public void rmr(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.deleteOnExit(path);
        System.out.println("Delete: " + folder);
        fs.close();
    }

    //下载文件或文件夹到本地系统
    public void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from " + remote + " to " + local);
        fs.close();
    }

    //启动函数
    public static void main(String[] args) throws IOException {
        JobConf conf = setConfig();
        HDFSDemo  hdfs = new HDFSDemo (conf);
//        hdfs.mkdirs("a/b/d");
//        hdfs.ls("a/b");
//        hdfs.copyFile("/home/yyq/a", "admin");
//        hdfs.ls("a/b");
//        hdfs.rmr("a/b");
//        hdfs.download("filesSpace", "/home/yyq/");
//        System.out.println("success!");


        //hdfs.ls("hdfs://192.168.1.104:9000/wgc/files");
        //hdfs.rmr("/wgc/files");
    }
}
