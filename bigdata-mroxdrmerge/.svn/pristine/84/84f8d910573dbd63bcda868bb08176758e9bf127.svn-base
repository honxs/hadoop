package cn.mastercom.bigdata.standalone.stat.uep;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import cn.mastercom.bigdata.standalone.local.FileReader;
import cn.mastercom.bigdata.standalone.local.FileTypeResult;
import cn.mastercom.bigdata.standalone.local.GridBuildingMap;
import cn.mastercom.bigdata.standalone.local.FileReader.LineHandler;
import cn.mastercom.bigdata.standalone.main.ATask;
import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.GridItemOfSize;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.uep.stat.UepStatDeals;
import cn.mastercom.bigdata.uep.stat.UepEnum;

public class UepTask extends ATask
{
	private Collection<File> m_files;
	private UepWork m_Work;
	private File m_dir;

	public UepTask(UepWork work, File dir, Collection<File> files)
	{
		m_Work = work;
		m_dir = dir;
		m_files = files;
	}

	@Override
	public List<Exception> run()
	{
		String dirName = m_dir.getName();
		String dateStr = dirName.substring(dirName.length() - 6, dirName.length());

		Date now = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("_HHmmss");
		
		FileTypeResult fileTypeResult = new FileTypeResult(m_Work.dstPath, dirName + sdf.format(now), dateStr, UepEnum.values());
		ResultOutputer resultOutputer = new ResultOutputer(fileTypeResult);
		UepStatDeals statDeals = new UepStatDeals(resultOutputer);

		List<Exception> es = new ArrayList<Exception>();

		for (File file : m_files)
		{
			try
			{
				doOne(statDeals, file);
			}
			catch (Exception e)
			{
				String msg = "文件:" + file.getAbsolutePath() + "\n";
				es.add(new Exception(msg, e));
			}
		}

		fileTypeResult.Flush();

		return es;
	}

	private void doOne(final UepStatDeals statDeals, File file) throws Exception
	{
		FileReader.readFileGBK(file.getAbsolutePath(), new LineHandler()
		{
			int lineIndex = 0;

			@Override
			public void handle(String line)
			{
				lineIndex++;

				if (lineIndex == 1) return;
				if (line.length() == 0) return;

				DT_Sample_4G sample = adpter(line);
				if (sample != null)
				{
					statDeals.dealSample(sample);
				}
			}
		});

		statDeals.outResult();
	}

	/**
	 * 适配
	 * 
	 * @param line
	 * @return
	 */
	private DT_Sample_4G adpter(String line)
	{
		try
		{
			String[] arrs = line.split("\t", -1);
			DT_Sample_4G sample = new DT_Sample_4G();
			sample.LteScEarfcn = 0;
			
			sample.itime = Integer.parseInt(arrs[0]);
			sample.ilongitude = Integer.parseInt(arrs[2]);
			sample.ilatitude = Integer.parseInt(arrs[3]);
			sample.IMSI = Long.parseLong(arrs[4]);
			sample.UserLabel = arrs[5];// wifi
			sample.ENBId = Integer.parseInt(arrs[6]);
			sample.Eci = Integer.parseInt(arrs[7]);
			sample.imeiTac = Integer.parseInt(arrs[8]);


			if (arrs[9].length() > 0) sample.LteScRSRP = Integer.parseInt(arrs[9]);
			if (arrs[10].length() > 0) sample.LteScRSRQ = Integer.parseInt(arrs[10]);
			if (arrs[11].length() > 0) sample.LteScPci = Integer.parseInt(arrs[11]);
			if (arrs[12].length() > 0) sample.LteScSinrUL = (int) Float.parseFloat(arrs[12]);
			if (arrs[13].length() > 0) sample.LteScEarfcn = Integer.parseInt(arrs[13]);

			sample.cityID = getCityID(sample.Eci);
			sample.ibuildingID = getBuildingID(sample.cityID, sample.ilongitude, sample.ilatitude);

			// sample.fileData(arrs);
			sample.ConfidenceType = StaticConfig.IM;
			sample.grid = new GridItemOfSize(sample.cityID, sample.ilongitude, sample.ilatitude, m_Work.indoorGridSize);

			return sample;
		}
		catch (Exception e)
		{
			return null;
		}
	}

	private int getCityID(long eci)
	{
		LteCellInfo cell = m_Work.cellConfig.getLteCell(eci);
		if (cell == null)
		{
			return -1;
		}
		else
		{
			return cell.cityid;
		}
	}

	private int getBuildingID(int cityID, int x, int y)
	{
		GridBuildingMap gridBuildingMap = m_Work.gridBuildingMap;
		if (gridBuildingMap == null)
		{
			return -1;
		}
		else
		{
			return gridBuildingMap.get(cityID, x, y);
		}
	}

	@Override
	public void runFinally() throws Exception
	{
		m_Work.backupHelper.backup(m_files);
	}

}
