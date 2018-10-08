package cn.mastercom.bigdata.standalone.local;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.standalone.local.FileReader.LineHandler;

public class FileMarker
{
	private byte[] lock = new byte[0]; // 特殊的instance变量

	static final long dayMillis = 24L * 3600 * 1000;

	int markDays;
	String markPath;
	private int m_srcPathLength = 0;

	private final String markFileBeginFlag = "mark";
	private final String markFileEndFlag = ".txt";

	/**
	 * 清理过期标记
	 * 
	 * @param dirs
	 *            : 文件夹,数据文件的父目录
	 */
	public void Clean(Collection<File> dirs)
	{
		if (!IOHelper.DirExist(markPath)) IOHelper.MakeDir(markPath);
		List<File> files = IOHelper.getFiles(markPath, "^" + markFileBeginFlag + ".*" + markFileEndFlag + "$", true);
		if (files != null && files.size() > 0)
		{
			Map<String, Integer> map = new HashMap<String, Integer>();
			for (File dir : dirs)
			{
				String file = getLogFileNameByDir(dir);
				String key = file.toLowerCase();
				map.put(key, 1);
			}

			for (File file : files)
			{
				long time = file.lastModified();
				String key = file.getAbsolutePath().toLowerCase();
				if (!map.containsKey(key) && time + markDays * dayMillis < System.currentTimeMillis())
				{
					file.delete();
				}
			}

		}

	}

	/**
	 * 获取路径对应的mark文件名
	 * 
	 * @param dir
	 * @return
	 */
	private String getLogFileNameByDir(File dir)
	{
		String name = dir.getAbsolutePath().substring(m_srcPathLength).replace('\\', '_').replace('/', '_');
		String path = IOHelper.pathCombine(markPath, markFileBeginFlag + name + markFileEndFlag);
		return IOHelper.GetAbsolutePath(path);
	}

	public FileMarker(String srcPath, String markPath, int markDays)
	{
		this.markPath = markPath;
		this.markDays = markDays;

		m_srcPathLength = IOHelper.GetAbsolutePath(srcPath).length();
	}

	/**
	 * 获取未做标记的文件
	 * 
	 * @param files
	 */
	public ArrayList<File> GetUnmarkedFiles(Collection<File> files)
	{
		Map<String, List<File>> dir_fileMap = groupByDir(files);
		ArrayList<File> results = new ArrayList<File>();

		for (List<File> kv : dir_fileMap.values())
		{
			ArrayList<File> ls = getUnmarkedFiles(kv);
			results.addAll(ls);
		}

		return results;
	}

	/**
	 * 按文件夹分组
	 * @param files
	 * @return
	 */
	private Map<String, List<File>> groupByDir(Collection<File> files)
	{
		Map<String, List<File>> dir_fileMap = new HashMap<String, List<File>>();

		for (File file : files)
		{
			String dir = file.getParent().toLowerCase();

			List<File> ls = null;
			if (dir_fileMap.containsKey(dir))
			{
				ls = dir_fileMap.get(dir);
			}
			else
			{
				ls = new ArrayList<File>();
				dir_fileMap.put(dir, ls);
			}

			ls.add(file);
		}

		return dir_fileMap;
	}

	/**
	 * 处理单个文件夹
	 * 
	 * @param files
	 *            : 同个文件夹下的文件
	 * @return
	 */
	private ArrayList<File> getUnmarkedFiles(List<File> files)
	{
		Map<String, Integer> map = readMarkFile(files.get(0).getParentFile());

		ArrayList<File> results = new ArrayList<File>();

		if (map == null)
		{
			results.addAll(files);
		}
		else
		{
			for (File file : files)
			{
				String key = file.getAbsolutePath().toLowerCase();
				if (!map.containsKey(key))
				{
					results.add(file);
				}
			}
		}

		return results;
	}

	/**
	 * 将已标记的文件读取到map
	 * 
	 * @param dir
	 * @return
	 */
	private HashMap<String, Integer> readMarkFile(File dir)
	{
		try
		{
			String markFile = getLogFileNameByDir(dir);

			if (IOHelper.FileExist(markFile))
			{
				final HashMap<String, Integer> map = new HashMap<String, Integer>();

				FileReader.readFileUTF8(markFile, new LineHandler()
				{
					@Override
					public void handle(String line)
					{
						String key = line.toLowerCase();
						map.put(key, 1);
					}
				});

				return map;
			}
		}
		catch (Exception e)
		{
		}
		return null;
	}

	/**
	 * 标记文件,支持多线程
	 * 
	 * @param files
	 *            : 文件
	 */
	public void MarkFile(Collection<File> files)
	{
		Map<String, List<File>> dir_fileMap = groupByDir(files);

		for (List<File> kv : dir_fileMap.values())
		{
			markFiles(kv);
		}
	}

	/**
	 * 标记文件,支持多线程
	 * 
	 * @param file
	 *            : 文件
	 */
	public void MarkFile(File file)
	{
		List<File> files = new ArrayList<File>();
		files.add(file);
		markFiles(files);
	}

	/**
	 * 将文件写到标记文件内
	 * 
	 * @param markFile
	 * @param files
	 *            : 同个文件夹下的文件
	 */

	private void markFiles(List<File> files)
	{
		synchronized (lock)
		{
			String markFile = getLogFileNameByDir(files.get(0).getParentFile());
			FileStore store = new FileStore(markFile);
			for (File file : files)
			{
				store.AddLine(file.getAbsolutePath());
			}
			store.Flush();
		}
	}
}
