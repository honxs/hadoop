package cn.mastercom.bigdata.util.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * 从文件系统中读取文本，支持文本格式和压缩文件
 * @author Kwong
 */
public final class FileReader {

	private static final Log LOG = LogFactory.getLog(FileReader.class);

	private FileReader(){}

	public interface LineHandler{
		
		void handle(String line);
	}
	
	private static FileStatus[] listFileStatus(FileSystem fs, String dirpath) throws Exception{
		
		Path directory = new Path(dirpath);
		if(!fs.exists(directory)){
			throw new FileNotFoundException(dirpath);
		}
		FileStatus[] fileStatusArr = fs.listStatus(directory, new PathFilter()
		{
			@Override
			public boolean accept(Path path)
			{
				if(path.getName().endsWith(".crc"))
					return false;
				else return true;
			}
		});
		return fileStatusArr;
	}
	
	/**
	 * 读取文件文本（支持压缩），并回调处理每行
	 * @param conf 配置
	 * @param dirpath 目录
	 * @param linehandler 回调处理
	 * @return
	 * @throws Exception
	 */
	public static boolean readFiles(Configuration conf, String dirpath, LineHandler linehandler) throws Exception
	{
		FileSystem fs = FileSystem.get(conf);

		FileStatus[] fileStatusArr = listFileStatus(fs, dirpath);
			
		for (FileStatus fileStatus : fileStatusArr)
		{
			if(!fileStatus.isDirectory())
				readFile(fs, fileStatus.getPath(), linehandler);
		}
		
		return true;
	}
	
	public static boolean readFiles(String dirpath, LineHandler linehandler) throws Exception{
		return readFiles(new Configuration(),dirpath, linehandler);
	}
	
	/**
	 * 读取文件文本（支持压缩），并回调处理每行
	 * @param conf 配置
	 * @param filePath 文件路径
	 * @param linehandler 每行回调处理
	 * @return
	 * @throws Exception
	 */
	public static boolean readFile(Configuration conf, String filePath, LineHandler linehandler) throws Exception
	{
		FileSystem fs = FileSystem.get(conf);

		Path file = new Path(filePath);
		if(!fs.exists(file)) {
			throw new FileNotFoundException(filePath);
		}
		
		return readFile(fs, file, linehandler);
	}
	
	/**
	 * 读取文件文本（支持压缩），并回调处理每行
	 * @param filePath 文件路径
	 * @param linehandler 每行回调处理
	 * @return
	 * @throws Exception
	 */
	public static boolean readFile(String filePath, LineHandler linehandler) throws Exception
	{
		return readFile(new Configuration(), filePath, linehandler);
	}
	
	/**
	 * 读取文件文本（支持压缩），并回调处理每行
	 * @param fs 文件系统
	 * @param file 文件路径
	 * @param linehandler 每行回调处理
	 * @return
	 * @throws Exception
	 */
	public static boolean readFile(FileSystem fs, Path file, LineHandler linehandler) throws Exception{
		
		//从文件名判断是否压缩
		final CompressionCodec codec = new CompressionCodecFactory(fs.getConf()).getCodec(file);

		String strData = null;
		
		try(BufferedReader reader = new BufferedReader(new InputStreamReader(codec == null ? fs.open(file) : codec.createInputStream(fs.open(file)), "UTF-8")))
		{
			while ((strData = reader.readLine()) != null)
			{
				if (strData.trim().length() == 0)
				{
					continue;
				}

				linehandler.handle(strData);
	
			}
		}
		catch (Exception e)
		{
			throw e;
		}
		return true;
	}
	
	
}
