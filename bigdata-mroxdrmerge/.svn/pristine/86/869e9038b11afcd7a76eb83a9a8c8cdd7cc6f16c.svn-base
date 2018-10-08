package cn.mastercom.bigdata.standalone.stat.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import cn.mastercom.bigdata.standalone.local.BackupHelper;
import cn.mastercom.bigdata.standalone.local.ConfigHelper;
import cn.mastercom.bigdata.standalone.local.GridBuildingMap;
import cn.mastercom.bigdata.standalone.local.IOHelper;
import cn.mastercom.bigdata.standalone.local.BackupHelper.BackupMode;
import cn.mastercom.bigdata.standalone.main.ATask;
import cn.mastercom.bigdata.standalone.main.IWork;
import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class TestWork implements IWork
{
	public static Logger LOG = Logger.getLogger(MainModel.class);

	private int threadCount;
	private int interval;
	
	public String srcPath;
	public String dstPath;

	public BackupHelper backupHelper;
	public int indoorGridSize;
	
	public CellConfig cellConfig;
	public GridBuildingMap gridBuildingMap;
	public ParseItem parseItem;
	
	@Override
	public String getName()
	{
		return "掌厅统计";
	}
	
	@Override
	public int getInterval()
	{
		return interval;
	}
	
	@Override
	public int getThreadCount()
	{
		return threadCount;
	}
	
	@Override
	public void init() throws Exception
	{
		parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("PalmHallApp");
		if (parseItem == null)
		{
			throw new IOException("parse item do not get.");
		}

		indoorGridSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getInDoorSize());

		ConfigHelper conf = new ConfigHelper("config/app_pha.xml");
		srcPath = conf.readAsString("//configuration/SrcPath");
		dstPath = conf.readAsString("//configuration/DstPath");
		String backupPath = conf.readAsString("//configuration/BackupPath");
		String lteCfgPath = conf.readAsString("//configuration/LteCellConfigPath");

		String buildingGridPath = conf.readAsString("//configuration/BuildingGridPath");

		threadCount = conf.readAsInteger("//configuration/ThreadCount", 1);
		if (threadCount < 1) threadCount = 1;

		interval = conf.readAsInteger("//configuration/Interval", 1);
		if (interval < 1) interval = 1;

		if (!IOHelper.DirExist(srcPath)) throw new Exception("SrcPath 输入路径不存在!");
		if (!IOHelper.DirExist(dstPath)) throw new Exception("DstPath 输出路径不存在!");
		if (!IOHelper.FileExist(lteCfgPath)) throw new Exception("LteCellConfigPath 小区参数文件不存在");
		
		backupHelper = new BackupHelper(srcPath, BackupMode.Mark, backupPath, 7);
		
		loadCell(lteCfgPath);
		loadBuildingGrid(buildingGridPath);
		
		
	}
	
	private void loadCell(String file)
	{
		LOG.info("开始加载小区数据:" + file);
		cellConfig = CellConfig.GetInstance();
		cellConfig.loadLteCell(file);
		LOG.info("加载小区数据完成:" + file);
	}

	private void loadBuildingGrid(String path) throws Exception
	{
		//String[] files = buildingGridFiles.split("\\|");// \\||\\*|\\? 表示用 | 或者 * 或者 ? 分割

		List<File> files = IOHelper.getFiles(path, "build_grid_.*\\.gz", true);
		if (files == null)
		{
			LOG.warn("没有获取到楼宇栅格数据文件");
		}
		else
		{
			gridBuildingMap = new GridBuildingMap(indoorGridSize, files);
		}
	}
	
	public Collection<ATask> getTasks() throws Exception
	{
		// UEPDATA_yyyyMMdd
		List<File> dirs = IOHelper.getDirs(srcPath, "_\\d{8}\\b");
		if (dirs == null || dirs.size() == 0) return null;

		backupHelper.Clean(dirs);
		Collection<ATask> tasks = new ArrayList<ATask>();
		for (File dir : dirs)
		{
			ATask task = getTask(dir);
			if (task != null)
			{
				tasks.add(task);
			}
		}

		return tasks;
	}

	private ATask getTask(File dir)
	{
		List<File> files = IOHelper.getFiles(dir.getAbsolutePath(), ".*", true);
		if (files == null || files.size() == 0)
		{
			return null;
		}
		
		List<File> ls = backupHelper.GetUsefulFiles(files, 10);
		if (ls.size() == 0) return null;
		
		return new TestTask(this, dir, ls);
	}

	@Override
	public void onTasksFinished() throws Exception
	{
		// TODO Auto-generated method stub
		
	}



}
