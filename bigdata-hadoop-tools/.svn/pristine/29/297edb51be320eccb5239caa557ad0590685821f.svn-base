package cn.mastercom.bigdata.util.redis;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import redis.clients.jedis.JedisCluster;

public class RedisUtil {
	 private static final String PATH = "C:\\Users\\DELL\\Desktop\\6323";
	 protected static final Log LOG = LogFactory.getLog(RedisUtil.class);
	    private static final ExecutorService POOL = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

	    public static void main(String[] args) {
	        long startTime = System.currentTimeMillis();
	        storage(PATH);
	        loadsAndUnzipFileFromRedis(JedisClusterFactory.getInstance().getJedisCluster(null, 0, null), "FINGERPRINT_OUT_162129153.txt.gz");
	        long finishTime = System.currentTimeMillis();
	        LOGHelper.GetLogger().writeLog(LogType.info, "总共耗时：" + (finishTime - startTime));
	   
	    }

	    /**
	     * 入库
	     *
	     * @param path 各地市指纹库文件的路径
	     */
	    public static void storage(String path) {
	        assert path != null && path.length() != 0;

	        String[] split = path.split("\\|" + File.separator);
	        String cityId = split[split.length - 1];
	        if (cityId == null)
	            throw new IllegalStateException("请检查输入的路径，确保输入的路径为xxx/地市ID/<indoor|inin|inout|ncell|out>");

	        // 保存 indoor
	        POOL.submit(new StorageTask(path + "/indoor", cityId + ".", ""));
	        // 保存 inin
	        POOL.submit(new StorageTask(path + "/inin"));
	        // 保存 inout
	        POOL.submit(new StorageTask(path + "/inout"));
	        // 保存 ncell
	        POOL.submit(new StorageTask(path + "/ncell"));
	        // 保存 out
	        POOL.submit(new StorageTask(path + "/out"));
	    }

	    /**
	     * 保存文件到redis集群中
	     *
	     * @param jc      jedisCluster对象
	     * @param dirPath 文件夹的路径
	     * @param prefix  redis对应的key的前缀
	     * @param suffix  redis对应的key的后缀
	     */
	    public static void saveFileToRedis(JedisCluster jc, String dirPath, String prefix, String suffix) {
	        assert jc != null;
	        assert dirPath != null && dirPath.length() != 0;
	        assert prefix != null && suffix != null;

	        File dir = new File(dirPath);
	        // 过滤掉文件夹
	        File[] files = dir.listFiles(new FileFilter() {
	            @Override
	            public boolean accept(File f) {
	                return f.isFile();
	            }

	        });

	        if (files == null || files.length == 0) return;

	        for (File f : files) {
	            saveFileToRedis(jc, f, prefix, suffix);
	        }

	    }

	    /**
	     * 保存文件到redis集群上
	     *
	     * @param jc     jedisCluster对象，用于操作redis集群
	     * @param f      目标文件
	     * @param prefix key的前缀
	     * @param suffix key的后缀
	     */
	    private static void saveFileToRedis(JedisCluster jc, File f, String prefix, String suffix) {

	        try (
	                FileInputStream fis = new FileInputStream(f);
	                ByteArrayOutputStream baos = new ByteArrayOutputStream()
	        ) {

	            IOUtils.copy(fis, baos);
	            byte[] value = baos.toByteArray();
	            jc.set((prefix + f.getName() + suffix).getBytes(), value);

	        } catch (IOException e) {
	        	LOGHelper.GetLogger().writeLog(LogType.error,"保存文件到redis出现问题", "保存文件到redis出现问题", e);
	        }
	    }

	    /**
	     * 从redis集群中加载GZ文件并解压缩，返回一个BufferedReader
	     *
	     * @param jc  jedisCluster对象
	     * @param key 文件名
	     * @return BufferedReader对象或<tt>NULL</tt>
	     */
	    public static BufferedReader loadsAndUnzipFileFromRedis(JedisCluster jc, String key) {
	        if (jc == null)
	        {
	        	LOGHelper.GetLogger().writeLog(LogType.error, "redis连接为空,无法获取BufferedReader! ");
	        	return null;
	        }
	        assert key != null && key.length() != 0;
	        ByteArrayInputStream bais = null;
	        GZIPInputStream gzis = null;
	        InputStreamReader isr = null;
            try {
            	byte[] bytes = jc.get(key.getBytes());
            	if (bytes == null)
            	{
            		LOGHelper.GetLogger().writeLog(LogType.error, "该key没有在redis找到!请检查 " + key);
            		return null;
            	}
            	else 
            	{
            		LOGHelper.GetLogger().writeLog(LogType.info, "该key在redis找到! " + key);
            	}
            	bais = new ByteArrayInputStream(bytes);
            	gzis = new GZIPInputStream(bais);
            	isr = new InputStreamReader(gzis);
	            return new BufferedReader(isr);
            } catch (IOException e) {
            	LOGHelper.GetLogger().writeLog(LogType.error,"创建BufferedReader出现异常", "创建BufferedReader出现异常", e);
				try	
				{
					if (bais != null) 
						bais.close();
				} 
				catch (Exception e0) 
				{ 
					
				}
            	return null;
            }

	    }
	    
	    /**
	     * 从集群中
	     * @author xmr
	     *
	     */
	    /**
	     * 从redis集群中加载非GZ文件，返回一个BufferedReader
	     *
	     * @param jc  jedisCluster对象
	     * @param key 文件名
	     * @return BufferedReader对象或<tt>NULL</tt>
	     */
	    public static BufferedReader loadsFileFromRedis(JedisCluster jc, String key) {
	        if (jc == null)
	        {
	        	LOGHelper.GetLogger().writeLog(LogType.error, "redis连接为空,无法获取BufferedReader! ");
	        	return null;
	        }
	        assert key != null && key.length() != 0;
	        ByteArrayInputStream bais = null;
	        InputStreamReader isr = null;
            try {
            	byte[] bytes = jc.get(key.getBytes());
            	if (bytes == null)
            	{
            		LOGHelper.GetLogger().writeLog(LogType.error, "该key没有在redis找到!请检查 " + key);
            	}
            	bais = new ByteArrayInputStream(bytes);
            	isr = new InputStreamReader(bais);
	            return new BufferedReader(isr);
            } catch (Exception e) {
            	LOGHelper.GetLogger().writeLog(LogType.error,"创建BufferedReader出现异常", "创建BufferedReader出现异常", e);
				try	
				{
					if (bais != null) 
						bais.close();
				} 
				catch (Exception e0) 
				{ 
					
				}
            	return null;
            }

	    }
	    static class StorageTask implements Runnable {

	        private String path;
	        private String prefix;
	        private String suffix;

	        private StorageTask() {
	            this.prefix = "";
	            this.suffix = "";
	        }

	        StorageTask(String path) {
	            this();
	            this.path = path;
	        }

	        StorageTask(String path, String prefix, String suffix) {
	            this();
	            this.path = path;
	            this.prefix = prefix;
	            this.suffix = suffix;
	        }

	        @Override
	        public void run() {
	            try (JedisCluster jc = JedisClusterFactory.getInstance().getJedisCluster(path, 0, path)) {
	                saveFileToRedis(jc, this.path, this.prefix, this.suffix);
	            } catch (IOException e) {
	            	LOGHelper.GetLogger().writeLog(LogType.error,"JedisCluster关闭出现异常", "JedisCluster关闭出现异常", e);
	            }
	        }
	    }
}