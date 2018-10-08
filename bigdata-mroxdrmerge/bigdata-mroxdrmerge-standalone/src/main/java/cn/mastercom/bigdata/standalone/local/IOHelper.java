package cn.mastercom.bigdata.standalone.local;

import java.io.File;
import java.io.FileFilter;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IOHelper
{

	/**
	 * 正则表达式搜索
	 * 
	 */
	class FileNameFilter implements FileFilter
	{
		Pattern m_pattern = null;
		boolean m_bFile = false;

		public FileNameFilter(String filter, boolean bFile)
		{
			m_pattern = Pattern.compile(filter, Pattern.CASE_INSENSITIVE);
			m_bFile = bFile;
		}

		@Override
		public boolean accept(File pathName)
		{
			if (m_bFile && pathName.isDirectory())
			{
				return true;
			}
			else if (!m_bFile && pathName.isFile())
			{
				return false;
			}
			else
			{
				Matcher m = m_pattern.matcher(pathName.getName());
				return m.find();
			}
		}

	}

	public static String GetAbsolutePath(String path)
	{
		try
		{
			File file = new File(path);
			return file.getAbsolutePath();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}

	public static boolean MakeDir(String path)
	{
		try
		{
			File file = new File(path);
			if (!file.exists())
			{
				return file.mkdirs();
			}
			return true;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * 获取目录下的所有文件夹
	 */
	public static List<File> getDirs(String path, String filter)
	{
		try
		{
			File file = new File(path);
			File[] files = file.listFiles((new IOHelper()).new FileNameFilter(filter, false));

			List<File> results = new ArrayList<File>();

			for (File f : files)
			{
				if (f.isDirectory())
				{
					results.add(f);
				}
			}

			return results;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}

	public static List<String> GetFiles(String path, String filter, boolean topDir)
	{
		try
		{
			File file = new File(path);
			File[] files = file.listFiles((new IOHelper()).new FileNameFilter(filter, true));

			List<String> results = new ArrayList<String>();

			for (File f : files)
			{
				if (f.isDirectory())
				{
					if (!topDir)
					{
						results.addAll(GetFiles(f.getAbsolutePath(), filter, topDir));
					}
				}
				else if (f.isFile())
				{
					results.add(f.getAbsolutePath());
				}
			}

			return results;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}

	public static List<File> getFiles(String path, String filter, boolean topDir)
	{
		try
		{
			File file = new File(path);
			File[] files = file.listFiles((new IOHelper()).new FileNameFilter(filter, true));

			List<File> results = new ArrayList<File>();

			for (File f : files)
			{
				if (f.isDirectory())
				{
					if (!topDir)
					{
						results.addAll(getFiles(f.getAbsolutePath(), filter, topDir));
					}
				}
				else if (f.isFile())
				{
					results.add(f);
				}
			}

			return results;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}

	public static String GetParantDir(String path)
	{
		try
		{
			File file = new File(path);
			return file.getParent();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}

	public static String GetPathName(String path)
	{
		try
		{
			File file = new File(path);
			return file.getName();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}

	public static boolean DeletePath(String path)
	{
		try
		{
			File file = new File(path);
			return deleteAllFilesOfDir(file);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
	}

	private static boolean deleteAllFilesOfDir(File path)
	{
		if (!path.exists())
		{
			return true;
		}

		if (path.isFile())
		{
			return path.delete();
		}

		File[] files = path.listFiles();
		for (int i = 0; i < files.length; i++)
		{
			deleteAllFilesOfDir(files[i]);
		}
		return path.delete();
	}

	public static boolean FileExist(String path)
	{
		try
		{
			File file = new File(path);
			return file.exists() && file.isFile();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
	}

	public static boolean PathExist(String path)
	{
		try
		{
			File file = new File(path);
			return file.exists();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
	}

	public static boolean DirExist(String path)
	{
		try
		{
			File file = new File(path);
			return file.exists() && file.isDirectory();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
	}

	public static boolean MakeParentDir(String path)
	{
		try
		{
			File file = new File(path);
			File parent = file.getParentFile();
			if (!parent.exists())
			{
				return parent.mkdirs();
			}
			return true;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
	}

	public static boolean MovePath(String oldPath, String newPath)
	{
		try
		{
			if (oldPath.equals(newPath)) return true;

			DeletePath(newPath);
			MakeParentDir(newPath);

			File oldfile = new File(oldPath);
			File newfile = new File(newPath);

			return oldfile.renameTo(newfile);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
	}

	public static String getTraceMessage(Exception e)
	{
		StringBuilder sb = new StringBuilder();
		sb.append(e.toString());
		sb.append("\n");
		StackTraceElement[] trace = e.getStackTrace();
		for (StackTraceElement traceElement : trace)
		{
			sb.append("\tat ");
			sb.append(traceElement.toString());
			sb.append("\n");
		}
		return sb.toString();
	}

	/**
	 * 连接路径
	 * 
	 * @param paths
	 *            不能为null
	 * @return
	 */
	public static String pathCombine(String... paths)
	{
		try
		{
			String path = paths[0];
			path = path.replace('\\', '/').trim();
			boolean flag = path.startsWith("/");

			for (int i = 1; i < paths.length; i++)
			{
				path = pathCombineX(path, paths[i]);
			}
			path = stringTrim(path, '/');
			if (flag) path = "/" + path;
			return path;
		}
		catch (Exception e)
		{
			return null;
		}
	}

	/**
	 * 
	 * @param path1
	 *            已处理好的路径
	 * @param path2
	 *            将要处理的路径
	 * @return
	 * @throws Exception
	 */
	private static String pathCombineX(String path1, String path2) throws Exception
	{
		if (path2 == null)
		{
			throw new Exception("用来连接的路径不能为null!");
		}

		path1 = stringTrim(path1, ' ', '/');
		path2 = stringTrim(path2.replace('\\', '/'), ' ', '/');

		if (path2 == "")
		{
			throw new Exception("用来连接的路径不能为空!");
		}

		return path1 + '/' + path2;
	}

	/**
	 * 去掉首尾字符
	 * 
	 * @param str
	 *            不能为null
	 * @param args
	 * @return
	 */
	public static String stringTrim(String str, char... args)
	{
		str = str.trim();
		if (args == null || args.length == 0)
		{
			return str;
		}

		str = trimStart(str, args);
		return trimEnd(str, args);
	}

	/**
	 * 去掉首部字符
	 * 
	 * @param str
	 *            不能为null
	 * @param args
	 *            不能为null或0长度
	 * @return
	 */
	public static String trimStart(String str, char... args)
	{
		int length = str.length();
		int index = 0;
		while (index < length)
		{
			char ch = str.charAt(index);
			if (!arrayContains(args, ch))
			{
				return str.substring(index);
			}
			index++;
		}
		return "";
	}

	/**
	 * 去掉尾部字符
	 * 
	 * @param str
	 *            不能为null
	 * @param args
	 *            不能为null或0长度
	 * @return
	 */
	public static String trimEnd(String str, char... args)
	{
		int length = str.length();
		int index = length - 1;

		while (index >= 0)
		{
			char ch = str.charAt(index);
			if (!arrayContains(args, ch))
			{
				return str.substring(0, index + 1);
			}
			index--;
		}
		return "";
	}

	private static boolean arrayContains(char[] array, char ch)
	{
		for (char c : array)
		{
			if (c == ch) return true;
		}
		return false;
	}

	public static long getLongFromStringByMd5(String str)
	{
		try
		{
			byte[] buffer = md5(str);
			long value = bytesToLong(buffer, 0);
			if (value < 0) return Long.MAX_VALUE + value + 1;
			return value;
		}
		catch (Exception e)
		{
			return 0;
		}
	}

	public static byte[] md5(String str) throws Exception
	{
		MessageDigest md = MessageDigest.getInstance("MD5");
		return md.digest(str.getBytes("utf-8"));
	}

	public static long bytesToLong(byte[] input, int offset)
	{
		if (offset < 0 || offset + 8 > input.length) throw new IllegalArgumentException(
				String.format("less than 8 bytes from index %d  is insufficient for long", offset));
		ByteBuffer buffer = ByteBuffer.wrap(input, offset, 8);
		// if (littleEndian)
		// {
		// // ByteBuffer.order(ByteOrder)
		// // 方法指定字节序,即大小端模式(BIG_ENDIAN/LITTLE_ENDIAN)
		// // ByteBuffer 默认为大端(BIG_ENDIAN)模式
		// buffer.order(ByteOrder.LITTLE_ENDIAN);
		// }
		return buffer.getLong();
	}

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception
	{
		long a = getLongFromStringByMd5("13812341234");//715731114220982836
	}
}
