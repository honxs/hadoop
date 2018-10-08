package cn.mastercom.bigdata.util.hadoop.hdfs;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * 写到文件系统
 * @author Kwong
 * 
 */
public final class FileWriter {

	private static final Log LOG = LogFactory.getLog(FileWriter.class);

	private FileWriter(){};

	public interface LineGetter extends Iterator<String>{
		
	}
	
	/**
	 * 简单的行获取回调 
	 * @param <T>
	 */
	public abstract static class AbstractLineGetter<T> implements LineGetter{

		Iterator<T> iterator;
		
		public AbstractLineGetter(Iterable<T> iteable){
			iterator = iteable.iterator();
		}
		
		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public String next() {
			return toLine(iterator.next());
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
		protected abstract String toLine(T t);
		
	}
	
	/**
	 * 输出文本文件（支持压缩），并自定义对象如何转化为一行
	 * @param filePath 文件路径，如为压缩格式后缀会自动压缩
	 * @param lineGetter 获取每行的回调
	 * @return
	 * @throws Exception
	 */
	public static boolean writeToFile(String filePath, LineGetter lineGetter) throws Exception
	{
		return writeToFile(new Configuration(), filePath, true, lineGetter);
	}
	
	/**
	 * 输出文本文件（支持压缩），并自定义对象如何转化为一行
	 * @param conf 配置
	 * @param filePath 文件路径，如为压缩格式后缀会自动压缩
	 * @param lineGetter 获取每行的回调
	 * @return
	 * @throws Exception
	 */
	public static boolean writeToFile(Configuration conf, String filePath, LineGetter lineGetter) throws Exception
	{
		return writeToFile(conf, filePath, true, lineGetter);
	}
	
	/**
	 * 输出文本文件（支持压缩），并自定义对象如何转化为一行
	 * @param filePath 文件路径，如为压缩格式后缀会自动压缩
	 * @param overwrite 文件存在是否覆盖 true：覆盖，false：报错
	 * @param lineGetter 获取每行的回调
	 * @return
	 * @throws Exception
	 */
	public static boolean writeToFile(String filePath, boolean overwrite, LineGetter lineGetter) throws Exception
	{
		return writeToFile(new Configuration(), filePath, overwrite, lineGetter);
	}
	
	/**
	 * 输出文本文件（支持压缩），并自定义对象如何转化为一行
	 * @param conf 配置
	 * @param filePath 文件路径，如为压缩格式后缀会自动压缩
	 * @param overwrite 文件存在是否覆盖 true：覆盖，false：报错
	 * @param lineGetter 获取每行的回调
	 * @return
	 * @throws Exception
	 */
	public static boolean writeToFile(Configuration conf, String filePath, boolean overwrite, LineGetter lineGetter) throws Exception
	{
		return writeToFile(FileSystem.get(conf), filePath, overwrite, lineGetter);
	}
	
	/**
	 * 输出文本文件（支持压缩），并自定义对象如何转化为一行
	 * @param fs 文件系统
	 * @param filePath 文件路径，如为压缩格式后缀会自动压缩
	 * @param overwrite 文件存在是否覆盖 true：覆盖，false：报错
	 * @param lineGetter 获取每行的回调
	 * @return
	 * @throws Exception
	 */
	public static boolean writeToFile(FileSystem fs, String filePath, boolean overwrite, LineGetter lineGetter) throws Exception{

		Path file = new Path(filePath);
		
		//从文件名判断是否压缩
		final CompressionCodec codec = new CompressionCodecFactory(fs.getConf()).getCodec(file);
		//不写出.crc文件
		fs.setWriteChecksum(false);
		
		try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(codec != null ? codec.createOutputStream(fs.create(file, overwrite)) : fs.create(file, overwrite), "utf-8"))){
			
			writeLines(writer, lineGetter);
			
		}catch (Exception e)
		{
			throw e;
		}
		return true;
	}
	
	/**
	 * 追加到文本文件（支持压缩），并自定义对象如何转化为一行
	 * @param filePath 文件路径，如为压缩格式后缀会自动压缩
	 * @param lineGetter 获取每行的回调
	 * @return
	 * @throws Exception 不支持windows文件系统
	 */
	public static boolean appendToFile(String filePath, LineGetter lineGetter) throws Exception{
		return appendToFile(new Configuration(), filePath, lineGetter);
	}
	
	/**
	 * 追加到文本文件（支持压缩），并自定义对象如何转化为一行
	 * @param conf 配置
	 * @param filePath 文件路径，如为压缩格式后缀会自动压缩
	 * @param lineGetter 获取每行的回调
	 * @return
	 * @throws Exception 不支持windows文件系统
	 */
	public static boolean appendToFile(Configuration conf, String filePath, LineGetter lineGetter) throws Exception{
		return appendToFile(FileSystem.get(conf), filePath, lineGetter);
	}
	
	/**
	 * 追加到文本文件（支持压缩），并自定义对象如何转化为一行
	 * @param fs 文件系统
	 * @param filePath 文件路径，如为压缩格式后缀会自动压缩
	 * @param lineGetter 获取每行的回调
	 * @return
	 * @throws Exception 不支持windows文件系统
	 */
	public static boolean appendToFile(FileSystem fs, String filePath, LineGetter lineGetter) throws Exception{
		fs.getConf().setBoolean("dfs.support.append", true);
		try{
			Path file = new Path(filePath);
			
			FSDataOutputStream os = null;
			//没有该文件就创建
			if(!fs.exists(file)){
				os = fs.create(file);
			}else{
				os = fs.append(file);
			}
			
			//从文件名判断是否压缩
			boolean isCompressedFile;
			
			final CompressionCodec codec = new CompressionCodecFactory(fs.getConf()).getCodec(file);
			
			if (null == codec) {
				isCompressedFile = false;
			}else{
				isCompressedFile = true;
			}
			
			try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(isCompressedFile ? codec.createOutputStream(os) : os, "utf-8"))){
				
				writeLines(writer, lineGetter);
							
			}catch (Exception e)
			{
				throw e;
			}
		}catch(Exception e){
			throw e;
		}finally{			
			fs.getConf().setBoolean("dfs.support.append", false);
		}
		return true;
		
	}
	
	private static void writeLines(BufferedWriter writer, LineGetter lineGetter) throws Exception{
		
		if(writer == null || lineGetter == null){
			throw new IllegalArgumentException();
		}
		
		String strData = null;
		
		while(lineGetter.hasNext()){
			
			strData = lineGetter.next();
			
			if(strData == null || strData.length() == 0)
				continue;
			
			writer.write(strData);
			
			writer.newLine();
			
		}
	}
}
