package cn.mastercom.util;

import lombok.NonNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by Kwong on 2018/7/17.
 */
public class FileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    public static void read(File file){
        //第一步 获取通道
        FileInputStream fis = null;
        FileChannel channel=null;
        try {
            fis = new FileInputStream(file);
            channel=fis.getChannel();
            //文件内容的大小
            int size=(int) channel.size();

            //第二步 指定缓冲区
            ByteBuffer buffer=ByteBuffer.allocate(1024);
            //第三步 将通道中的数据读取到缓冲区中
            channel.read(buffer);

            Buffer bf= buffer.flip();

            byte[] bt=buffer.array();
            System.out.println(new String(bt,0,size));

            buffer.clear();
            buffer=null;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            try {
                channel.close();
                fis.close();

            } catch (IOException e) {
                e.printStackTrace();
            }


        }
    }

    public static void readFileByLine(String filePath, LineHandler handler) throws FileNotFoundException {
        readFileByLine(new File(filePath), ByteBuffer.allocate(4096), handler);
    }

    public static void readFileByLine(File file,
                                      ByteBuffer rBuffer,  LineHandler handler) throws FileNotFoundException {
        readFileByLine(new FileInputStream(file).getChannel(), rBuffer, "UTF-8", handler);
    }

    public static void readFileByLine(FileChannel fcin,
                                      ByteBuffer rBuffer, String encode, LineHandler handler) {
        String enter = "\n";
//        List<String> dataList = new ArrayList<String>();//存储读取的每行数据
        byte[] lineByte = new byte[0];

//        String encode = "GBK";
//		String encode = "UTF-8";
        try {
            //temp：由于是按固定字节读取，在一次读取中，第一行和最后一行经常是不完整的行，因此定义此变量来存储上次的最后一行和这次的第一行的内容，
            //并将之连接成完成的一行，否则会出现汉字被拆分成2个字节，并被提前转换成字符串而乱码的问题
            byte[] temp = new byte[0];
            while (fcin.read(rBuffer) != -1) {//fcin.read(rBuffer)：从文件管道读取内容到缓冲区(rBuffer)
                int rSize = rBuffer.position();//读取结束后的位置，相当于读取的长度
                byte[] bs = new byte[rSize];//用来存放读取的内容的数组
                rBuffer.rewind();//将position设回0,所以你可以重读Buffer中的所有数据,此处如果不设置,无法使用下面的get方法
                rBuffer.get(bs);//相当于rBuffer.get(bs,0,bs.length())：从position初始位置开始相对读,读bs.length个byte,并写入bs[0]到bs[bs.length-1]的区域
                rBuffer.clear();

                int startNum = 0;
                int LF = 10;//换行符
                int CR = 13;//回车符
                boolean hasLF = false;//是否有换行符
                for(int i = 0; i < rSize; i++){
                    if(bs[i] == LF){
                        hasLF = true;
                        int tempNum = temp.length;
                        int lineNum = i - startNum;
                        lineByte = new byte[tempNum + lineNum];//数组大小已经去掉换行符

                        System.arraycopy(temp, 0, lineByte, 0, tempNum);//填充了lineByte[0]~lineByte[tempNum-1]
                        temp = new byte[0];
                        System.arraycopy(bs, startNum, lineByte, tempNum, lineNum);//填充lineByte[tempNum]~lineByte[tempNum+lineNum-1]

                        String line = new String(lineByte, 0, lineByte.length, encode);//一行完整的字符串(过滤了换行和回车)
//                        dataList.add(line);
//						System.out.println(line);
//                        writeFileByLine(fcout, wBuffer, line + enter);
                        handler.handle(line);

                        //过滤回车符和换行符
                        if(i + 1 < rSize && bs[i + 1] == CR){
                            startNum = i + 2;
                        }else{
                            startNum = i + 1;
                        }

                    }
                }
                if(hasLF){
                    temp = new byte[bs.length - startNum];
                    System.arraycopy(bs, startNum, temp, 0, temp.length);
                }else{//兼容单次读取的内容不足一行的情况
                    byte[] toTemp = new byte[temp.length + bs.length];
                    System.arraycopy(temp, 0, toTemp, 0, temp.length);
                    System.arraycopy(bs, 0, toTemp, temp.length, bs.length);
                    temp = toTemp;
                }
            }
            if(temp != null && temp.length > 0){//兼容文件最后一行没有换行的情况
                String line = new String(temp, 0, temp.length, encode);
//                dataList.add(line);
//				System.out.println(line);
//                writeFileByLine(fcout, wBuffer, line + enter);
                handler.handle(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 写到文件上
     * @param fcout
     * @param wBuffer
     * @param line
     */
    @SuppressWarnings("static-access")
    public static void writeFileByLine(FileChannel fcout, ByteBuffer wBuffer,
                                       String line) {
        try {
            fcout.write(wBuffer.wrap(line.getBytes("UTF-8")), fcout.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public interface LineHandler{

        void handle(String line);
    }

    public static boolean checkFileExists(String filePath, FileSystem fs){
        Path path = new Path(filePath);
        try {
            if (fs == null){
                Configuration conf = new Configuration();
//            conf.set("fs.defaultFS","hdfs://192.168.1.31:9000");
                fs = FileSystem.get(conf);
            }

            return fs.exists(path) && !fs.isDirectory(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 兼容本地 或 集群环境的输入流换取
     *
     * @param filePath
     * @return
     */
    public static InputStream getInputStreamFrom(String filePath, FileSystem fs){
        Path path = new Path(filePath);
        try {
            if (fs == null){
                Configuration conf = new Configuration();
//            conf.set("fs.defaultFS","hdfs://192.168.1.31:9000");
                fs = FileSystem.get(conf);
            }
            if (!fs.exists(path))
                throw new FileNotFoundException(path + "该文件（夹）不存在");
            return fs.open(path);
        } catch (IOException e) {
            LOG.error("获取输入流异常", e);
        }
        return null;
    }

    public static OutputStream getOutputStreamTo(String filePath, FileSystem fs){
        Path path = new Path(filePath);
        try {
            if (fs == null){
                Configuration conf = new Configuration();
//            conf.set("fs.defaultFS","hdfs://192.168.1.31:9000");
                fs = FileSystem.get(conf);
            }
            return fs.create(path);
        } catch (IOException e) {
            LOG.error("获取输出流异常", e);
        }
        return null;
    }

    public static FileStatus[] listTxtFileFrom(@NonNull String dirPath, FileSystem fs) {
        Path f = new Path(dirPath);
        try {
            if (!fs.isDirectory(f)) throw new RuntimeException("请输入一个文件夹路径");
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            return fs.listStatus(f, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    String name = path.getName();
                    return name.endsWith(".txt") && name.matches("^[1-9].*");
                }
            });
        } catch (IOException e) {
            LOG.error("列举文件异常", e);
        }

        return new FileStatus[0];
    }

    public static boolean delete(String filePath, FileSystem fs){
        Path path = new Path(filePath);
        try {
            if (fs == null){
                Configuration conf = new Configuration();
//            conf.set("fs.defaultFS","hdfs://192.168.1.31:9000");
                fs = FileSystem.get(conf);
            }
            return fs.delete(path, false);
        }catch (FileNotFoundException e) {
            return true;
        }catch (IOException e) {
            LOG.error("删除文件异常", e);
            return false;
        }
    }
}
