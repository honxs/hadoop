package cn.mastercom.bigdata.standalone.stat.test;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
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
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.pha.stat.PhaStatDeals;
import cn.mastercom.bigdata.pha.stat.PhaEnum;

public class TestTask extends ATask
{

	private Collection<File> m_files;
	private TestWork m_Work;
	private File m_dir;

	public TestTask(TestWork work, File dir, Collection<File> files)
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

		FileTypeResult fileTypeResult = new FileTypeResult(m_Work.dstPath, dirName, dateStr, PhaEnum.values());
		ResultOutputer resultOutputer = new ResultOutputer(fileTypeResult);
		PhaStatDeals statDeals = new PhaStatDeals(resultOutputer);

		DataAdapterReader dataAdapterReader = new DataAdapterReader(m_Work.parseItem);

		List<Exception> es = new ArrayList<Exception>();

		for (File file : m_files)
		{
			try
			{
				doOne(statDeals, file, dataAdapterReader);
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

	private void doOne(final PhaStatDeals statDeals, File file, final DataAdapterReader dataAdapterReader)
			throws Exception
	{
		FileReader.readFileGBK(file.getAbsolutePath(), new LineHandler()
		{
			@Override
			public void handle(String line)
			{
				if (line.length() == 0) return;

				dataAdapterReader.readData(line);
				DT_Sample_4G sample = adpter(dataAdapterReader);
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
	private DT_Sample_4G adpter(DataAdapterReader dataAdapterReader)
	{
		try
		{
			DT_Sample_4G sample = new DT_Sample_4G();

			parse(sample, dataAdapterReader);

			if (sample.Eci == 0)
			{
				return null;
			}
			else
			{
				sample.cityID = getCityID(sample.Eci);
				sample.ibuildingID = getBuildingID(sample.cityID, sample.ilongitude, sample.ilatitude);

				sample.ConfidenceType = StaticConfig.IM;
				sample.grid = new GridItemOfSize(sample.cityID, sample.ilongitude, sample.ilatitude,
						m_Work.indoorGridSize);

				return sample;
			}
		}
		catch (Exception e)
		{
			return null;
		}
	}

	private void parse(DT_Sample_4G sample, DataAdapterReader reader) throws ParseException
	{
		long time = reader.GetDateValue("Time").getTime();
		sample.itime = (int) (time / 1000);
		sample.wtimems = (short) (time % 1000);

		sample.ilongitude = reader.GetIntValue("Longitude", 0);
		sample.ilatitude = reader.GetIntValue("Latitude", 0);
		sample.IMSI = reader.GetLongValue("IMSI", 0);
		sample.UserLabel = reader.GetStrValue("Wifilist", "");// wifi
		sample.ENBId = reader.GetIntValue("EnbID", 0);
		sample.Eci = reader.GetIntValue("Eci", 0);
		sample.imeiTac = reader.GetIntValue("ImeiTac", 0);

		if (sample.ENBId == 0)
		{
			sample.ENBId = (int) (sample.Eci / 256);
		}
		else if (sample.Eci < 256)
		{
			sample.Eci = sample.ENBId * 256 + sample.Eci;
		}

		sample.LteScRSRP = reader.GetIntValue("LteScRSRP", StaticConfig.Int_Abnormal);

		sample.LteScRSRQ = reader.GetIntValue("LteScRSRQ", StaticConfig.Int_Abnormal);

		sample.LteScPci = reader.GetIntValue("LteScPci", StaticConfig.Int_Abnormal);

		sample.LteScSinrUL = reader.GetIntValue("LteScSinr", StaticConfig.Int_Abnormal);

		sample.LteScEarfcn = reader.GetIntValue("LteScEarfcn", StaticConfig.Int_Abnormal);
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
		//backupFiles(m_Work.backupPath, m_dir.getName(), m_files);
		//backupDir(m_Work.backupPath, m_dir);
		//m_Work.fileMarker.MarkFiles(m_dir, m_files);
		m_Work.backupHelper.backup(m_files);
	}

}
