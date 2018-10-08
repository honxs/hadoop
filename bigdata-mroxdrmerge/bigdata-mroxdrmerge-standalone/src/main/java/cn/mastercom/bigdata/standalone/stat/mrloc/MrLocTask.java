package cn.mastercom.bigdata.standalone.stat.mrloc;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import cn.mastercom.bigdata.standalone.local.FileReader;
import cn.mastercom.bigdata.standalone.local.FileTypeResult;
import cn.mastercom.bigdata.standalone.local.GridBuildingMap;
import cn.mastercom.bigdata.standalone.local.IOHelper;
import cn.mastercom.bigdata.standalone.local.FileReader.LineHandler;
import cn.mastercom.bigdata.standalone.main.ATask;
import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.GridItemOfSize;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.mrloc.stat.MrLocEnum;
import cn.mastercom.bigdata.mrloc.stat.MrLocStatDeals;

public class MrLocTask extends ATask
{

	private Collection<File> m_files;
	private MrLocWork m_Work;
	private File m_dir;

	public MrLocTask(MrLocWork work, File dir, Collection<File> files)
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

		FileTypeResult fileTypeResult = new FileTypeResult(m_Work.dstPath, dirName + sdf.format(now), dateStr,
				MrLocEnum.values());
		ResultOutputer resultOutputer = new ResultOutputer(fileTypeResult);
		MrLocStatDeals statDeals = new MrLocStatDeals(resultOutputer);

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

	private void doOne(final MrLocStatDeals statDeals, File file, final DataAdapterReader dataAdapterReader)
			throws Exception
	{
		FileReader.readFileUTF8(file.getAbsolutePath(), new LineHandler()
		{
			int lineIndex = 0;

			@Override
			public void handle(String line)
			{
				lineIndex++;

				if (lineIndex == 1) return;
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
				if (sample.ibuildingID < 0)
				{
					sample.ConfidenceType = StaticConfig.OM;
				}
				else
				{
					sample.ConfidenceType = StaticConfig.IM;
				}

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
		sample.fileID = reader.GetIntValue("fileID", 0);
		sample.itime = reader.GetIntValue("itime", 0);
		sample.wtimems = (short) (reader.GetLongValue("wtimems", 0) % 1000);

		sample.ilongitude = reader.GetIntValue("ilongitude", 0);
		sample.ilatitude = reader.GetIntValue("ilatitude", 0);
		sample.iLAC = reader.GetIntValue("iLAC", 0);
		sample.iCI = reader.GetLongValue("iCI", 0);
		sample.Eci = reader.GetLongValue("Eci", 0);
		sample.IMSI = IOHelper.getLongFromStringByMd5(reader.GetStrValue("IMSI", ""));
		sample.MSISDN = reader.GetStrValue("MSISDN", "");
		sample.UETac = reader.GetStrValue("UETac", "");
		sample.UEBrand = reader.GetStrValue("UEBrand", "");
		sample.UEType = reader.GetStrValue("UEType", "");
		sample.serviceType = reader.GetIntValue("serviceType", 0);
		sample.serviceSubType = reader.GetIntValue("serviceSubType", 0);
		sample.urlDomain = reader.GetStrValue("urlDomain", "");
		
		sample.IPDataUL = reader.GetLongValue("IPDataUL", StaticConfig.Int_Abnormal);
		sample.IPDataDL=reader.GetLongValue("IPDataDL", StaticConfig.Int_Abnormal);
		sample.duration=reader.GetIntValue("duration", StaticConfig.Int_Abnormal);
		sample.IPThroughputUL=reader.GetDoubleValue("IPThroughputUL", StaticConfig.Int_Abnormal);
		sample.IPThroughputDL=reader.GetDoubleValue("IPThroughputDL", StaticConfig.Int_Abnormal);
		sample.IPPacketUL=reader.GetIntValue("IPPacketUL", StaticConfig.Int_Abnormal);
		sample.IPPacketDL=reader.GetIntValue("IPPacketDL", StaticConfig.Int_Abnormal);
		sample.TCPReTranPacketUL=reader.GetIntValue("TCPReTranPacketUL", StaticConfig.Int_Abnormal);
		sample.TCPReTranPacketDL=reader.GetIntValue("TCPReTranPacketDL", StaticConfig.Int_Abnormal);
		sample.sessionRequest=(int)(reader.GetLongValue("sessionRequest", 0)/1000);
		sample.sessionResult=(int)(reader.GetLongValue("sessionResult", 0)/1000);
		sample.eventType=reader.GetIntValue("eventType", 0);
		sample.userType=reader.GetIntValue("userType", 0);
		sample.eNBName=reader.GetStrValue("eNBName", "");
		sample.eNBLongitude=reader.GetIntValue("eNBLongitude", 0);
		sample.eNBLatitude=reader.GetIntValue("eNBLatitude", 0);
		sample.eNBDistance=reader.GetIntValue("eNBDistance", 0);
		sample.flag=reader.GetStrValue("flag", "");
		sample.ENBId=reader.GetIntValue("ENBId", 0);
		sample.UserLabel=reader.GetStrValue("UserLabel", "");
		sample.CellId=reader.GetLongValue("CellId", 0);
		sample.Earfcn=reader.GetIntValue("Earfcn", 0);
		sample.SubFrameNbr=reader.GetIntValue("SubFrameNbr", 0);
		sample.MmeCode=reader.GetIntValue("MmeCode", 0);
		sample.MmeGroupId=reader.GetIntValue("MmeGroupId", 0);
		sample.MmeUeS1apId=reader.GetLongValue("MmeUeS1apId", 0);
		
		sample.LteScRSRP=reader.GetIntValue("LteScRSRP", StaticConfig.Int_Abnormal);
		sample.LteScRSRQ=reader.GetIntValue("LteScRSRQ", StaticConfig.Int_Abnormal);
		sample.LteScEarfcn=reader.GetIntValue("LteScEarfcn", 0);
		sample.LteScPci=reader.GetIntValue("LteScPci", StaticConfig.Int_Abnormal);
		sample.LteScBSR=reader.GetIntValue("LteScBSR", StaticConfig.Int_Abnormal);
		sample.LteScRTTD=reader.GetIntValue("LteScRTTD", StaticConfig.Int_Abnormal);
		sample.LteScTadv=reader.GetIntValue("LteScTadv", StaticConfig.Int_Abnormal);
		sample.LteScAOA=reader.GetIntValue("LteScAOA", StaticConfig.Int_Abnormal);
		sample.LteScPHR=reader.GetIntValue("LteScPHR", StaticConfig.Int_Abnormal);
		sample.LteScRIP=reader.GetIntValue("LteScRIP", StaticConfig.Int_Abnormal);
		sample.LteScSinrUL=reader.GetIntValue("LteScSinrUL", StaticConfig.Int_Abnormal);
		
		sample.nccount[0] = (short)reader.GetIntValue("nccount0", 0);
		sample.nccount[1] = (short)reader.GetIntValue("nccount1", 0);
//		sample.nccount[2] = (short)reader.GetIntValue("nccount2", 0);
//		sample.nccount[3] = (short)reader.GetIntValue("nccount3", 0);
		
		for (int j = 0; j < 6; j++)
		{
			sample.tlte[j].LteNcRSRP = reader.GetIntValue("LteNcRSRP" + j, StaticConfig.Int_Abnormal);
			sample.tlte[j].LteNcRSRQ = reader.GetIntValue("LteNcRSRQ" + j, StaticConfig.Int_Abnormal);
			sample.tlte[j].LteNcEarfcn = reader.GetIntValue("LteNcEarfcn" + j, StaticConfig.Int_Abnormal);
			sample.tlte[j].LteNcPci = reader.GetIntValue("LteNcPci" + j, StaticConfig.Int_Abnormal);
			
			if (sample.tlte[j].LteNcRSRP != StaticConfig.Int_Abnormal) sample.tlte[j].LteNcRSRP -= 141;
			if (sample.tlte[j].LteNcRSRQ != StaticConfig.Int_Abnormal) sample.tlte[j].LteNcRSRQ = (sample.tlte[j].LteNcRSRQ - 40 ) / 2;
		}
//		for (int j = 0; j < 6; j++)
//		{
//			sample.ttds[j].TdsPccpchRSCP = reader.GetIntValue("TdsPccpchRSCP" + j, StaticConfig.Int_Abnormal);
//			sample.ttds[j].TdsNcellUarfcn = reader.GetIntValue("TdsNcellUarfcn" + j, 0);
//			sample.ttds[j].TdsCellParameterId = reader.GetIntValue("TdsCellParameterId" + j, 0);
//			
//			if (sample.ttds[j].TdsPccpchRSCP != StaticConfig.Int_Abnormal) sample.ttds[j].TdsPccpchRSCP -= 141;
//		}

		for (int j = 0; j < 6; j++)
		{
			sample.tgsm[j].GsmNcellCarrierRSSI = reader.GetIntValue("GsmNcellCarrierRSSI" + j, StaticConfig.Int_Abnormal);
			sample.tgsm[j].GsmNcellBcch = reader.GetIntValue("GsmNcellBcch" + j, 0);
			sample.tgsm[j].GsmNcellBsic = reader.GetIntValue("GsmNcellBsic" + j, 0);
			
			if (sample.tgsm[j].GsmNcellCarrierRSSI != StaticConfig.Int_Abnormal) sample.tgsm[j].GsmNcellCarrierRSSI -= 141;
		}
		
		sample.dist=reader.GetIntValue("dist", 0);
		sample.radius=reader.GetIntValue("radius", 0);
		sample.locType=reader.GetStrValue("loctp", "");
		
		//-----------------------------------
		if (sample.LteScRSRP != StaticConfig.Int_Abnormal) sample.LteScRSRP -= 141;
		if (sample.LteScRSRQ != StaticConfig.Int_Abnormal) sample.LteScRSRQ = (sample.LteScRSRQ - 40 ) / 2;
		if (sample.LteScSinrUL != StaticConfig.Int_Abnormal) sample.LteScSinrUL -= 11;
		if (sample.ENBId == 0)
		{
			sample.ENBId = (int) (sample.Eci / 256);
		}
		else if (sample.Eci < 256)
		{
			sample.Eci = sample.ENBId * 256 + sample.Eci;
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
