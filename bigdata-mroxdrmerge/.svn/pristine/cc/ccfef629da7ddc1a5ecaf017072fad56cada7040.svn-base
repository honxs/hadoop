package cn.mastercom.bigdata.standalone.local;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class BackupHelper
{
	public static Logger LOG = Logger.getLogger(MainModel.class);

	public enum BackupMode
	{
		Move, Mark,
	}

	private FileMarker m_fileMarker = null;
	private int m_srcPathLength = 0;
	private String m_backupPath = null;
	private BackupMode m_backupMode = BackupMode.Move;

	/**
	 * 
	 * @param srcPath
	 *            :源路径
	 * @param backupMode
	 *            : 备份模式
	 * @param backupPath
	 *            : 备份路径
	 * @param markDays
	 *            : Mark模式下,保存多久的mark
	 * @throws Exception
	 */
	public BackupHelper(String srcPath, BackupMode backupMode, String backupPath, int markDays) throws Exception
	{
		m_backupMode = backupMode;
		if (IOHelper.DirExist(backupPath))
		{
			m_backupPath = backupPath;
		}

		m_srcPathLength = IOHelper.GetAbsolutePath(srcPath).length();

		switch (m_backupMode)
		{
			case Move:
				if (m_backupPath == null)
				{
					LOG.info("备份目录不存在,已处理的文件将会直接删除.");
				}
				break;

			case Mark:
				if (m_backupPath == null)
				{
					throw new Exception("Mark 模式下,备份目录必须存在!");
				}
				m_fileMarker = new FileMarker(srcPath, backupPath, markDays);
				break;

			default:
				break;
		}
	}

	public void Clean(Collection<File> dirs)
	{
		if (m_fileMarker == null) return;
		m_fileMarker.Clean(dirs);
	}

	/**
	 * 获取有效的文件
	 * 
	 * @param files
	 *            : 文件
	 * @param seconds
	 *            : 最后修改时间限制, 0表示不限制
	 * @return
	 */
	public ArrayList<File> GetUsefulFiles(Collection<File> files, int seconds)
	{
		if (seconds > 0)
		{
			ArrayList<File> ls = getUsefulFiles(files, seconds);
			if (m_fileMarker == null)
			{
				return ls;
			}
			else
			{
				return m_fileMarker.GetUnmarkedFiles(ls);
			}
		}
		else
		{
			if (m_fileMarker == null)
			{
				ArrayList<File> results = new ArrayList<File>();
				results.addAll(files);
				return results;
			}
			else
			{
				return m_fileMarker.GetUnmarkedFiles(files);
			}
		}

	}

	private ArrayList<File> getUsefulFiles(Collection<File> files, int seconds)
	{
		ArrayList<File> results = new ArrayList<File>();
		long now = System.currentTimeMillis();

		for (File file : files)
		{
			long time = file.lastModified();
			if (time + 1000L * seconds < now)
			{
				results.add(file);
			}
		}
		return results;
	}

	/**
	 * 备份
	 * 
	 * @param paths
	 *            : 文件(夹)
	 */
	public void backup(Collection<File> paths)
	{
		for (File file : paths)
		{
			backup(file);
		}
	}
	
	/**
	 * 备份
	 * 
	 * @param path
	 *            : 文件(夹)
	 */
	public void backup(File path)
	{
		switch (m_backupMode)
		{
			case Move:
				do_backup(m_backupPath, path, m_srcPathLength);
				break;

			case Mark:
				m_fileMarker.MarkFile(path);
				break;

			default:
				break;
		}
	}


	/**
	 * 备份
	 * 
	 * @param backupPath
	 *            : 备份根路径,不备份直接删除则传 null
	 * 
	 * @param path
	 *            : 需要备份的文件(夹)
	 * 
	 * @param srcPathLength
	 *            : 根目录长度
	 */

	private void do_backup(String backupPath, File path, int srcPathLength)
	{
		if (backupPath == null)
		{
			IOHelper.DeletePath(path.getAbsolutePath());
		}
		else
		{

			String subPath = path.getAbsolutePath().substring(srcPathLength);
			String newPath = IOHelper.pathCombine(backupPath, subPath);
			IOHelper.MovePath(path.getAbsolutePath(), newPath);
		}
	}

}
