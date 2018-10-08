package cn.mastercom.bigdata.util.hadoop.hdfs;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 根据通配符适配路径
 * @author Kwong
 * 
 * pattern仅支持以下：
 * ? Matches any single character. 
 * * Matches zero or more characters. 
 * [abc]  Matches a single character from character set {a,b,c}. 
 * [a-b]  Matches a single character from the character range {a...b}. Note that character a must be lexicographically less than or equal to character b. 
 * [^a]  Matches a single character that is not from character set or range {a}. Note that the ^ character must occur immediately to the right of the opening bracket. 
 * \c  Removes (escapes) any special meaning of character c. 
 * {ab,cd}  Matches a string from the string set {ab, cd}  
 * {ab,c{de,fh}}  Matches a string from the string set {ab, cde, cfh} 
 */
public final class FileMatcher {

	private static final Log LOG = LogFactory.getLog(FileMatcher.class);
	/**
	 * 匹配路径，并返回路径数组
	 * @param pattern
	 * @param filter
	 * @param fs
	 * @return
	 */
	public static FileStatus[] matchFiles(String pattern, PathFilter filter, FileSystem fs){
		FileStatus[] result =  new FileStatus[0];
		try {

			if (filter != null) {
				result =  fs.globStatus(new Path(pattern), filter);
			} else {
				result = fs.globStatus(new Path(pattern));
			}
		}catch(Exception e){
			LOG.error(String.format("ERROR: No path found with pattern[%s]", pattern), e);
		}
		return result == null ? new FileStatus[0] : result;
	}
	
	/**
	 * 匹配路径，并返回路径数组
	 * @param pattern 匹配模式
	 * @return
	 */
	public static Path[] matchPaths(String pattern){
		return matchPaths(pattern, new Configuration());
	}
	
	/**
	 * 匹配路径，并返回路径数组
	 * @param pattern 匹配模式
	 * @param conf 配置
	 * @return
	 */
	public static Path[] matchPaths(String pattern, Configuration conf){
		try {
			return matchPaths(pattern, FileSystem.get(conf));
		}catch (IOException e){
			LOG.error("ERROR: Can not get FileSystem with conf", e);
			return new Path[0];
		}
	}
	
	/**
	 * 匹配路径，并返回路径数组
	 * @param pattern 匹配模式
	 * @param fs 文件系统
	 * @return
	 */
	public static Path[] matchPaths(String pattern, FileSystem fs){
		
		FileStatus[] files = matchFiles(pattern, null, fs);
		Path[] paths = new Path[files.length];
		for(int i = 0 ; i<  files.length ; i++){
			paths[i] = files[i].getPath();
		}
		return paths;
	}
	
	/**
	 * 匹配路径，并返回路径字符串数组
	 * @param pattern 匹配模式
	 * @return
	 */
	public static String[] matchPathStrs(String pattern){
		return matchPathStrs(pattern, new Configuration());
	}
	
	/**
	 * 匹配路径，并返回路径字符串数组
	 * @param pattern 匹配模式
	 * @param conf 配置
	 * @return
	 */
	public static String[] matchPathStrs(String pattern, Configuration conf) {
		try {
			return matchPathStrs(pattern, FileSystem.get(conf));
		} catch (IOException e) {
			LOG.error("ERROR: Can not get FileSystem with conf", e);
			return new String[0];
		}
	}
	
	/**
	 * 匹配路径，并返回路径字符串数组
	 * @param pattern 匹配模式
	 * @param fs 文件系统
	 * @return
	 */
	public static String[] matchPathStrs(String pattern, FileSystem fs){
		
		FileStatus[] files = matchFiles(pattern, null, fs);
		String[] paths = new String[files.length];
		for(int i = 0 ; i <  files.length ; i++){
			paths[i] = files[i].getPath().toString();
		}
		return paths;
	}
	
	/**
	 * 匹配路径，并返回以分隔符拼接字符串
	 * @param pattern 匹配模式
	 * @param separator 分隔符
	 * @return
	 */
	public static String matchPathStr(String pattern, String separator) throws IOException {
		return matchPathStr(pattern, new Configuration(), separator);
	}
	
	/**
	 * 匹配路径，并返回以分隔符拼接字符串
	 * @param pattern 匹配模式
	 * @param conf 配置
	 * @param separator 分隔符
	 * @return
	 */
	public static String matchPathStr(String pattern, Configuration conf, String separator) throws IOException {
		return matchPathStr(pattern, FileSystem.get(conf), separator);
	}
	
	/**
	 * 匹配路径，并返回以分隔符拼接字符串
	 * @param pattern 匹配模式
	 * @param fs 文件系统
	 * @param separator 分隔符
	 * @return
	 */
	public static String matchPathStr(String pattern, FileSystem fs, String separator){
		
		FileStatus[] files = matchFiles(pattern, null, fs);
		/***** 兼容老代码使用冒号来判断，所以将hdfs://master:9000删掉 *****/
		//TODO 将来删掉
		String fsPath = fs.getUri().toString();

		StringBuilder sb = new StringBuilder();
		if(files.length > 0){
			for(int i = 0 ; i <  files.length - 1 ; i++){
				sb.append( files[i].getPath().toString().replace(fsPath, "")).append(separator);
			}
			sb.append( files[files.length - 1].getPath().toString().replace(fsPath, ""));
		}
		return sb.toString();
	}

	public static void main(String [] args) throws IOException {
		System.out.println(matchPathStr(args[0],","));
	}

}
