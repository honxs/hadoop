package cn.mastercom.bigdata.StructData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cn.mastercom.bigdata.base.model.DO;
import cn.mastercom.bigdata.evt.locall.model.MroErrorEventData;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.Func;

public class DT_Sample_4G implements DO
{
	public static final int NCCOUNT_ARRAY_LENGTH = 2;
	public static final int NCLTE_ARRAY_LENGTH = 6;
	public static final int NCGSM_ARRAY_LENGTH = 3;
//	public static final int TRIP_ARRAY_LENGTH = 10;
	public static final int DXFREQ_ARRAY_LENGTH = 3;
	public static final int LTFREQ_ARRAY_LENGTH = 3;
	
	
	public final short[] nccount = new short[NCCOUNT_ARRAY_LENGTH];
	public final NC_LTE[] tlte = new NC_LTE[NCLTE_ARRAY_LENGTH];
	public final NC_GSM[] tgsm = new NC_GSM[NCGSM_ARRAY_LENGTH];
//	public final SC_FRAME[] trip = new SC_FRAME[TRIP_ARRAY_LENGTH];
	public final NC_LTE[] dx_freq = new NC_LTE[DXFREQ_ARRAY_LENGTH];
	public final NC_LTE[] lt_freq = new NC_LTE[LTFREQ_ARRAY_LENGTH];
	public int cityID;
	public int fileID;
	public int imeiTac;// 保存imei
	public int itime;
	public short wtimems;
	public byte bms;
	public int ilongitude;
	public int ilatitude;
	public int ibuildingID;// 楼宇id
	public short iheight;// 楼层
	public int iLAC;
	public long iCI;
	public long Eci;
	public long IMSI;
	public String MSISDN;
	public String UETac;
	public String UEBrand;
	public String UEType;
	public int serviceType;
	public int serviceSubType;
	public String urlDomain;
	public long IPDataUL;
	public long IPDataDL;
	public int duration;
	public double IPThroughputUL;
	public double IPThroughputDL;
	public int IPPacketUL;
	public int IPPacketDL;
	public int TCPReTranPacketUL;
	public int TCPReTranPacketDL;
	public int sessionRequest;
	public int sessionResult;
	public int eventType;
	public int userType;
	public String eNBName;
	public int eNBLongitude;
	public int eNBLatitude;
	public int eNBDistance;
	public String flag;
	public int ENBId;
	public String UserLabel;
	public long CellId;
	public int Earfcn;
	public int SubFrameNbr;
	public int MmeCode;
	public int MmeGroupId;
	public long MmeUeS1apId;
	public int Weight;
	public int LteScRSRP;
	public int LteScRSRQ;
	public int LteScEarfcn;
	public int LteScPci;
	public int LteScBSR;
	public int LteScRTTD;
	public int LteScTadv;
	public int LteScAOA;
	public int LteScPHR;
	public double LteScRIP;
	public int LteScSinrUL;
	
	//yzx add at 20180403
//	public NC_LTE[] freq;	//异频
	public int LocFillType;// 0是正常值，1是经纬度回填

	public int testType;
	public int location;
	public long dist;
	public int radius;
	public String locType;
	public int indoor;

	public String networktype;
	public String label;// static,low,unknow,high

	public int simuLongitude;
	public int simuLatitude;

	public int moveDirect;
	public String mrType;

	public int dfcnJamCellCount;// 异频干扰小区个数
	public int sfcnJamCellCount;// 同频干扰小区个数

	public int LteScPUSCHPRBNum;
	public int LteScPDSCHPRBNum;
	public int LteSceNBRxTxTimeDiff;
	// new add 前后5分钟小区切换信息
	public String eciSwitchList;
	// -------------------------------------------mr新统计分YD/LT/DX 2017.6.14

	// 新添加字段表示经纬度来源和运动状态
	public String wifilist;
	public String xdrid = "";// 关联XDRID
	public String wifimac1;// wifi地址
	public int wifirssi1;// wifi强度
	public String wifimac2;// wifi地址
	public int wifirssi2;// wifi强度
//	public int LteScRSRP_DX; // 电信主服场强
//	public int LteScRSRQ_DX; // 电信主服信号质量
//	public int LteScEarfcn_DX; // 电信主服频点
//	public int LteScPci_DX; // 电信主服PCI
//	public int LteScRSRP_LT; // 联通主服场强
//	public int LteScRSRQ_LT; // 联通主服信号质量
//	public int LteScEarfcn_LT; // 联通主服频点
//	public int LteScPci_LT; // 联通主服PCI
	public int locSource;// 位置精度 high /mid /low
	public int samState;// int or out
	// 重叠覆盖
	public int overlapSameEarfcn = 1;
	public int OverlapAll = 1;
	// 场景统计
	public int iAreaType = -1;
	public int iAreaID = -1;
	// mdt 置信度
	public int Confidence;

	public int ConfidenceType;// 置信度类型

	public GridItemOfSize grid;
	public int fullNetType;// 全网通类型

	public int sectionId;
	public int segmentId;
	public long trainKey;

	public LteScPlrQciData qciData;
	
	//yzx add at 20180310
	public int position;// 方位

	public boolean isResidentUser;

	public static final String spliter = "\t";
	
	

	public DT_Sample_4G()
	{
//		nccount = new short[4];
//		tlte = new NC_LTE[6];
//		tgsm = new NC_GSM[6];
//		trip = new SC_FRAME[10];
//		lt_freq = new NC_LTE[3];
//		dx_freq = new NC_LTE[3];
		Clear();
	}

	public void Clear()
	{
		//20180315
		cityID = -1;
		fileID = -1;
		imeiTac = StaticConfig.Int_Abnormal;
		itime = StaticConfig.Int_Abnormal;
		wtimems = 0;
		bms = 0;
		ilongitude = 0;
		ilatitude = 0;
		ibuildingID = -1;
		iheight = -1;
		iLAC = StaticConfig.Int_Abnormal;
		iCI = StaticConfig.Int_Abnormal;
		Eci = StaticConfig.Long_Abnormal;
		IMSI = StaticConfig.Long_Abnormal;
		serviceType = StaticConfig.Int_Abnormal;
		serviceSubType = StaticConfig.Int_Abnormal;
		IPDataUL = StaticConfig.Long_Abnormal;
		IPDataDL = StaticConfig.Long_Abnormal;
		duration = StaticConfig.Int_Abnormal;
		IPThroughputUL = StaticConfig.Double_Abnormal;
		IPThroughputDL = StaticConfig.Double_Abnormal;
		IPPacketUL = StaticConfig.Int_Abnormal;
		IPPacketDL = StaticConfig.Int_Abnormal;
		TCPReTranPacketUL = StaticConfig.Int_Abnormal;
		TCPReTranPacketDL = StaticConfig.Int_Abnormal;
		sessionRequest = StaticConfig.Int_Abnormal;
		sessionResult = StaticConfig.Int_Abnormal;
		eventType = StaticConfig.Int_Abnormal;
		userType = StaticConfig.Int_Abnormal;
		eNBLongitude = StaticConfig.Int_Abnormal;
		eNBLatitude = StaticConfig.Int_Abnormal;
		eNBDistance = StaticConfig.Int_Abnormal;
		SubFrameNbr = StaticConfig.Int_Abnormal;
		eciSwitchList = "";
		wifilist = "";
		xdrid = "";// 关联XDRID
		wifimac1 = "";// wifi地址
		wifirssi1 = StaticConfig.Int_Abnormal;// wifi强度
		wifimac2 = "";// wifi地址
		wifirssi2 = StaticConfig.Int_Abnormal;// wifi强度
//		LteScRSRP_DX = StaticConfig.Int_Abnormal; // 电信主服场强
//		LteScRSRQ_DX = StaticConfig.Int_Abnormal; // 电信主服信号质量
//		LteScEarfcn_DX = StaticConfig.Int_Abnormal; // 电信主服频点
//		LteScPci_DX = StaticConfig.Int_Abnormal; // 电信主服PCI
//		LteScRSRP_LT = StaticConfig.Int_Abnormal; // 联通主服场强
//		LteScRSRQ_LT = StaticConfig.Int_Abnormal; // 联通主服信号质量
//		LteScEarfcn_LT = StaticConfig.Int_Abnormal; // 联通主服频点
//		LteScPci_LT = StaticConfig.Int_Abnormal; // 联通主服PCI
		locSource = StaticConfig.Int_Abnormal;// 位置精度 high /mid /low
		samState = StaticConfig.Int_Abnormal;// int or out
		iAreaType = -1;
		iAreaID = -1;
		Confidence = StaticConfig.Int_Abnormal;
		ConfidenceType = StaticConfig.Int_Abnormal;// 置信度类型
		fullNetType = StaticConfig.Int_Abnormal;// 全网通类型
		sectionId = StaticConfig.Natural_Abnormal;
		segmentId = StaticConfig.Natural_Abnormal;
		trainKey = StaticConfig.Natural_Abnormal;
		
		
		ENBId = StaticConfig.Int_Abnormal;
		CellId = StaticConfig.Long_Abnormal;
		Earfcn = StaticConfig.Int_Abnormal;
		MmeCode = StaticConfig.Int_Abnormal;
		MmeGroupId = StaticConfig.Int_Abnormal;
		MmeUeS1apId = StaticConfig.Int_Abnormal;
		Weight = StaticConfig.Int_Abnormal;
		LteScRSRP = StaticConfig.Int_Abnormal;
		LteScRSRQ = StaticConfig.Int_Abnormal;
		LteScEarfcn = StaticConfig.Int_Abnormal;
		LteScPci = StaticConfig.Int_Abnormal;
		LteScBSR = StaticConfig.Int_Abnormal;
		LteScRTTD = StaticConfig.Int_Abnormal;
		LteScTadv = StaticConfig.Int_Abnormal;
		LteScAOA = StaticConfig.Int_Abnormal;
		LteScPHR = StaticConfig.Int_Abnormal;
		LteScRIP = StaticConfig.Double_Abnormal;
		LteScSinrUL = StaticConfig.Int_Abnormal;
		LteScPUSCHPRBNum = StaticConfig.Int_Abnormal;
		LteScPDSCHPRBNum = StaticConfig.Int_Abnormal;
		LteSceNBRxTxTimeDiff = StaticConfig.Int_Abnormal;
		grid = null;
		
		for (int i = 0; i < nccount.length; i++)
		{
			nccount[i] = 0;
		}

		for (int i = 0; i < 6; i++)
		{
			tlte[i] = new NC_LTE();
			tlte[i].Clear();
//			ttds[i] = new NC_TDS();
//			ttds[i].Clear();
//			tgsmall[i] = new NC_GSM();
//			tgsmall[i].Clear();
		}

//		for (int i = 0; i < trip.length; i++)
//		{
//			trip[i] = new SC_FRAME();
//			trip[i].Clear();
//		}
		
		for (int i = 0; i < 3; i++)
		{
			tgsm[i] = new NC_GSM();
			tgsm[i].Clear();
			dx_freq[i] = new NC_LTE();
			dx_freq[i].Clear();
			lt_freq[i] = new NC_LTE();
			lt_freq[i].Clear();
		}
		
		overlapSameEarfcn = 1;
		OverlapAll = 1;

		testType = -1;

		MSISDN = "";
		UETac = "";
		UEBrand = "";
		UEType = "";
		urlDomain = "";
		eNBName = "";
		flag = "";
		UserLabel = "";
		locType = "";
		networktype = "";
		label = "";

		moveDirect = -1;
		mrType = "";

		LocFillType = 0;

		location = -1;
		dist = -1;
		radius = -1;
		locType = "";
		indoor = -1;

		simuLongitude = 0;
		simuLatitude = 0;

		sfcnJamCellCount = 0;
		dfcnJamCellCount = 0;

		position = 0;
		isResidentUser = false;
	}

	public boolean isOriginalLoction()
	{
		if (indoor >= 0)
		{
			return true;
		}
		return false;
	}
	
	public List<NC_LTE> getNclte_Freq()
	{
		List<NC_LTE> itemList = new ArrayList<NC_LTE>();
		for (NC_LTE nc_LTE : lt_freq)
		{
			if (nc_LTE.LteNcRSRP > -141)
			{
				itemList.add(nc_LTE);
			}
		}
		for (NC_LTE nc_LTE : dx_freq)
		{
			if (nc_LTE.LteNcRSRP > -141)
			{
				itemList.add(nc_LTE);
			}
		}
		return itemList;
	}

//	public List<NC_LTE> getNclte_Freq()
//	{
//		List<NC_LTE> itemList = new ArrayList<NC_LTE>();
//		for (int i = 0; i < 9; ++i)
//		{
//			NC_LTE resLte = null;
//			if (i <= 4)
//			{
//				if (tgsm[i + 1].GsmNcellBcch > 0)
//				{
//					resLte = new NC_LTE();
//					resLte.LteNcEarfcn = tgsm[i + 1].GsmNcellBcch;
//					resLte.LteNcPci = tgsm[i + 1].GsmNcellBsic;
//					resLte.LteNcRSRP = tgsm[i + 1].GsmNcellCarrierRSSI / 1000 - 200;
//					resLte.LteNcRSRQ = tgsm[i + 1].GsmNcellCarrierRSSI % 1000 - 200;
//				}
//				else
//				{
//					break;
//				}
//			}
//			else
//			{
//				if (ttds[i - 3].TdsNcellUarfcn > 0)
//				{
//					resLte = new NC_LTE();
//					resLte.LteNcEarfcn = ttds[i - 3].TdsNcellUarfcn;
//					resLte.LteNcPci = ttds[i - 3].TdsCellParameterId;
//					resLte.LteNcRSRP = ttds[i - 3].TdsPccpchRSCP / 1000 - 200;
//					resLte.LteNcRSRQ = ttds[i - 3].TdsPccpchRSCP % 1000 - 200;
//				}
//				else
//				{
//					break;
//				}
//			}
//
//			if (resLte != null)
//			{
//				itemList.add(resLte);
//			}
//		}
//
//		return itemList;
//	}

	public String createAreaSampleToLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(cityID);
		bf.append(spliter);
		bf.append(iAreaType);
		bf.append(spliter);
		bf.append(iAreaID);
		bf.append(spliter);
		bf.append(itime);
		bf.append(spliter);
		bf.append(wtimems);
		bf.append(spliter);
		bf.append(Eci);
		bf.append(spliter);
		bf.append(ibuildingID);
		bf.append(spliter);
		bf.append(iheight);
		bf.append(spliter);
		bf.append(ilongitude);
		bf.append(spliter);
		bf.append(ilatitude);
		bf.append(spliter);
		bf.append(IMSI);
		bf.append(spliter);
		bf.append(MSISDN);
		bf.append(spliter);
		bf.append(MmeUeS1apId);
		bf.append(spliter);
		bf.append(xdrid);
		bf.append(spliter);
		bf.append(UserLabel);// wifilist
		bf.append(spliter);
		bf.append(imeiTac);
		bf.append(spliter);
		bf.append(LteScRSRP);
		bf.append(spliter);
		bf.append(LteScRSRQ);
		bf.append(spliter);
		bf.append(LteScEarfcn);
		bf.append(spliter);
		bf.append(LteScPci);
		bf.append(spliter);
		bf.append(LteScBSR);
		bf.append(spliter);
		bf.append(LteScRTTD);
		bf.append(spliter);
		bf.append(LteScTadv);
		bf.append(spliter);
		bf.append(LteScAOA);
		bf.append(spliter);
		bf.append(LteScPHR);
		bf.append(spliter);
		bf.append(LteScRIP);
		bf.append(spliter);
		bf.append(LteScSinrUL);
		bf.append(spliter);
		for (int i = 0; i < nccount.length; i++)
		{
			bf.append(nccount[i]);
			bf.append(spliter);
		}
		for (NC_LTE nc_lte : tlte)
		{
			bf.append(nc_lte.LteNcRSRP);
			bf.append(spliter);
			bf.append(nc_lte.LteNcRSRQ);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEarfcn);
			bf.append(spliter);
			bf.append(nc_lte.LteNcPci);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEci);
			bf.append(spliter);
		}
		for (int i = 0; i < 3; i++)
		{
			bf.append(tgsm[i].GsmNcellCarrierRSSI);
			bf.append(spliter);
			bf.append(tgsm[i].GsmNcellBcch);
			bf.append(spliter);
			bf.append(tgsm[i].GsmNcellBsic);
			bf.append(spliter);
			bf.append(tgsm[i].GsmNcellLac);
			bf.append(spliter);
			bf.append(tgsm[i].GsmNcellCi);
			bf.append(spliter);
		}
		
		for (NC_LTE nc_lte : dx_freq)
		{
			bf.append(nc_lte.LteNcRSRP);
			bf.append(spliter);
			bf.append(nc_lte.LteNcRSRQ);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEarfcn);
			bf.append(spliter);
			bf.append(nc_lte.LteNcPci);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEci);
			bf.append(spliter);
		}
		
		for (NC_LTE nc_lte : lt_freq)
		{
			bf.append(nc_lte.LteNcRSRP);
			bf.append(spliter);
			bf.append(nc_lte.LteNcRSRQ);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEarfcn);
			bf.append(spliter);
			bf.append(nc_lte.LteNcPci);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEci);
			bf.append(spliter);
		}
		
		bf.append(overlapSameEarfcn);
		bf.append(spliter);
		bf.append(OverlapAll);
		bf.append(spliter);
		bf.append(mrType);
		bf.append(spliter);
		bf.append(locType);
		bf.append(spliter);
		bf.append(location);
		bf.append(spliter);
		bf.append(moveDirect);
		bf.append(spliter);
		bf.append(dist);
		bf.append(spliter);
		bf.append(radius);
		bf.append(spliter);
		bf.append(label);

		return bf.toString();
	}

	public String createNewSampleToLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(cityID);
		bf.append(spliter);
		bf.append(itime);
		bf.append(spliter);
		bf.append(wtimems);
		bf.append(spliter);
		bf.append(Eci);
		bf.append(spliter);
		bf.append(ibuildingID);
		bf.append(spliter);
		bf.append(iheight);
		bf.append(spliter);
		bf.append(ilongitude);
		bf.append(spliter);
		bf.append(ilatitude);
		bf.append(spliter);
		bf.append(Func.getEncrypt(IMSI));
		bf.append(spliter);
		bf.append(Func.getEncrypt(MSISDN));
		bf.append(spliter);
		bf.append(MmeUeS1apId);
		bf.append(spliter);
		bf.append(xdrid);
		bf.append(spliter);
		bf.append(UserLabel);// wifilist
		bf.append(spliter);
		bf.append(imeiTac);
		bf.append(spliter);
		bf.append(LteScRSRP);
		bf.append(spliter);
		bf.append(LteScRSRQ);
		bf.append(spliter);
		bf.append(LteScEarfcn);
		bf.append(spliter);
		bf.append(LteScPci);
		bf.append(spliter);
		bf.append(LteScBSR);
		bf.append(spliter);
		bf.append(LteScRTTD);
		bf.append(spliter);
		bf.append(LteScTadv);
		bf.append(spliter);
		bf.append(LteScAOA);
		bf.append(spliter);
		bf.append(LteScPHR);
		bf.append(spliter);
		bf.append(LteScRIP);
		bf.append(spliter);
		bf.append(LteScSinrUL);
		bf.append(spliter);
		for (int i = 0; i < nccount.length; i++)
		{
			bf.append(nccount[i]);
			bf.append(spliter);
		}
		for (NC_LTE nc_lte : tlte)
		{
			bf.append(nc_lte.LteNcRSRP);
			bf.append(spliter);
			bf.append(nc_lte.LteNcRSRQ);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEarfcn);
			bf.append(spliter);
			bf.append(nc_lte.LteNcPci);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEci);
			bf.append(spliter);
		}
		for (int i = 0; i < 3; i++)
		{
			bf.append(tgsm[i].GsmNcellCarrierRSSI);
			bf.append(spliter);
			bf.append(tgsm[i].GsmNcellBcch);
			bf.append(spliter);
			bf.append(tgsm[i].GsmNcellBsic);
			bf.append(spliter);
			bf.append(tgsm[i].GsmNcellLac);
			bf.append(spliter);
			bf.append(tgsm[i].GsmNcellCi);
			bf.append(spliter);
		}
		
		for (NC_LTE nc_lte : dx_freq)
		{
			bf.append(nc_lte.LteNcRSRP);
			bf.append(spliter);
			bf.append(nc_lte.LteNcRSRQ);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEarfcn);
			bf.append(spliter);
			bf.append(nc_lte.LteNcPci);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEci);
			bf.append(spliter);
		}
		
		for (NC_LTE nc_lte : lt_freq)
		{
			bf.append(nc_lte.LteNcRSRP);
			bf.append(spliter);
			bf.append(nc_lte.LteNcRSRQ);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEarfcn);
			bf.append(spliter);
			bf.append(nc_lte.LteNcPci);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEci);
			bf.append(spliter);
		}
		
		bf.append(overlapSameEarfcn);
		bf.append(spliter);
		bf.append(OverlapAll);
		bf.append(spliter);
		bf.append(mrType);
		bf.append(spliter);
		bf.append(locType);
		bf.append(spliter);
		bf.append(location);
		bf.append(spliter);
		bf.append(moveDirect);
		bf.append(spliter);
		bf.append(dist);
		bf.append(spliter);
		bf.append(radius);
		bf.append(spliter);
		bf.append(label);
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			bf.append(spliter);
			bf.append(samState);
			bf.append(spliter);
			bf.append(testType);
		}
		return bf.toString();
	}

	public static DT_Sample_4G eventToSample(DT_Event event)
	{
		DT_Sample_4G sample = new DT_Sample_4G();
		sample.IMSI = event.imsi;
		sample.cityID = event.cityID;
		sample.fileID = event.fileID;
		sample.imeiTac = event.SampleID - 1;
		sample.itime = event.itime;
		sample.wtimems = event.wtimems;
		sample.bms = event.bms;
		sample.eventType = event.eventID > 29000 ? event.eventID - 29000 : event.eventID - 20000;
		sample.ilongitude = event.ilongitude;
		sample.ilatitude = event.ilatitude;
		sample.iLAC = event.iLAC;
		sample.Eci = event.iCI;
		sample.iCI = event.iCI;
		sample.ENBId = (int) (event.iCI / 256);
		sample.serviceType = (int) event.ivalue1;
		sample.serviceSubType = (int) event.ivalue2;
		sample.IMSI = event.ivalue3;
		sample.duration = (int) event.ivalue6;
		sample.IPDataUL = (event.ivalue4 * sample.duration) / 8000;
		sample.IPDataDL = (event.ivalue5 * sample.duration) / 8000;
		sample.testType = event.testType;
		sample.location = event.location;
		sample.dist = event.dist;
		sample.radius = event.radius;
		sample.locType = event.loctp;
		sample.indoor = event.indoor;
		sample.networktype = event.networktype;
		sample.label = event.label;
		sample.moveDirect = event.moveDirect;
		sample.flag = "EVT";
		return sample;
	}

	// zks add
	public void fillSamData(String[] values)
	{
		int i = 0;
		cityID = Integer.parseInt(values[i++]);
		imeiTac = Integer.parseInt(values[i++]);
		itime = Integer.parseInt(values[i++]);
		wtimems = Short.parseShort(values[i++]);
		bms = Byte.parseByte(values[i++]);
		ilongitude = Integer.parseInt(values[i++]);
		ilatitude = Integer.parseInt(values[i++]);
		ibuildingID = Integer.parseInt(values[i++]);
		i = Short.parseShort(values[i++]);
		iLAC = Integer.parseInt(values[i++]);
		iCI = Long.parseLong(values[i++]);
		Eci = Long.parseLong(values[i++]);

		IMSI = Long.parseLong(values[i++]);
		MSISDN = values[i++];

		ENBId = Integer.parseInt(values[i++]);
		UserLabel = values[i++];
		CellId = Long.parseLong(values[i++]);
		Earfcn = Integer.parseInt(values[i++]);

		MmeUeS1apId = Long.parseLong(values[i++]);

		LteScRSRP = Integer.parseInt(values[i++]);
		LteScRSRQ = Integer.parseInt(values[i++]);
		LteScEarfcn = Integer.parseInt(values[i++]);
		LteScPci = Integer.parseInt(values[i++]);
		LteScBSR = Integer.parseInt(values[i++]);

		LteScTadv = Integer.parseInt(values[i++]);
		LteScAOA = Integer.parseInt(values[i++]);

		LteScSinrUL = Integer.parseInt(values[i++]);
		nccount[0] = Short.parseShort(values[i++]);

		ilongitude = Integer.parseInt(values[values.length - 2]);
		ilatitude = Integer.parseInt(values[values.length - 1]);

		// zks 默认赋值的
		location = 7;
		locType = "lll";
		radius = 80;

	}

//	public void fileData(String values[])
//	{
//		int i = 0;
//		cityID = Integer.parseInt(values[i++]);
//		imeiTac = Integer.parseInt(values[i++]);
//		itime = Integer.parseInt(values[i++]);
//		wtimems = Short.parseShort(values[i++]);
//		bms = Byte.parseByte(values[i++]);
//		ilongitude = Integer.parseInt(values[i++]);
//		ilatitude = Integer.parseInt(values[i++]);
//		ibuildingID = Integer.parseInt(values[i++]);
//		i = Short.parseShort(values[i++]);
//		iLAC = Integer.parseInt(values[i++]);
//		iCI = Long.parseLong(values[i++]);
//		Eci = Long.parseLong(values[i++]);
//		IMSI = Long.parseLong(values[i++]);
//		MSISDN = values[i++];
//		UETac = values[i++];
//		UEBrand = values[i++];
//		UEType = values[i++];
//		serviceType = Integer.parseInt(values[i++]);
//		serviceSubType = Integer.parseInt(values[i++]);
//		urlDomain = values[i++];
//		IPDataUL = Long.parseLong(values[i++]);
//		IPDataDL = Long.parseLong(values[i++]);
//		duration = Integer.parseInt(values[i++]);
//		IPThroughputUL = Double.parseDouble(values[i++]);
//		IPThroughputDL = Double.parseDouble(values[i++]);
//		IPPacketUL = Integer.parseInt(values[i++]);
//		IPPacketDL = Integer.parseInt(values[i++]);
//		TCPReTranPacketUL = Integer.parseInt(values[i++]);
//		TCPReTranPacketDL = Integer.parseInt(values[i++]);
//		sessionRequest = Integer.parseInt(values[i++]);
//		sessionResult = Integer.parseInt(values[i++]);
//		try
//		{
//			eventType = Integer.parseInt(values[i++]);
//		}
//		catch (NumberFormatException e)
//		{
//			eventType = -1000000;
//		}
//		userType = Integer.parseInt(values[i++]);
//		eNBName = values[i++];
//		eNBLongitude = Integer.parseInt(values[i++]);
//		eNBLatitude = Integer.parseInt(values[i++]);
//		eNBDistance = Integer.parseInt(values[i++]);
//		flag = values[i++];
//		ENBId = Integer.parseInt(values[i++]);
//		UserLabel = values[i++];
//		CellId = Long.parseLong(values[i++]);
//		Earfcn = Integer.parseInt(values[i++]);
//		SubFrameNbr = Integer.parseInt(values[i++]);
//		MmeCode = Integer.parseInt(values[i++]);
//		MmeGroupId = Integer.parseInt(values[i++]);
//		MmeUeS1apId = Long.parseLong(values[i++]);
//		Weight = Integer.parseInt(values[i++]);
//		LteScRSRP = Integer.parseInt(values[i++]);
//		LteScRSRQ = Integer.parseInt(values[i++]);
//		LteScEarfcn = Integer.parseInt(values[i++]);
//		LteScPci = Integer.parseInt(values[i++]);
//		LteScBSR = Integer.parseInt(values[i++]);
//		LteScRTTD = Integer.parseInt(values[i++]);
//		LteScTadv = Integer.parseInt(values[i++]);
//		LteScAOA = Integer.parseInt(values[i++]);
//		LteScPHR = Integer.parseInt(values[i++]);
//		LteScRIP = Integer.parseInt(values[i++]);
//		LteScSinrUL = Integer.parseInt(values[i++]);
//		for (int j = 0; j < nccount.length; j++)
//		{
//			nccount[j] = Short.parseShort(values[i++]);
//		}
//		for (int j = 0; j < 6; j++)
//		{
//			tlte[j].LteNcRSRP = Integer.parseInt(values[i++]);
//			tlte[j].LteNcRSRQ = Integer.parseInt(values[i++]);
//			tlte[j].LteNcEarfcn = Integer.parseInt(values[i++]);
//			tlte[j].LteNcPci = Integer.parseInt(values[i++]);
//		}
//		for (int j = 0; j < 6; j++)
//		{
//			ttds[j].TdsPccpchRSCP = Integer.parseInt(values[i++]);
//			ttds[j].TdsNcellUarfcn = Integer.parseInt(values[i++]);
//			ttds[j].TdsCellParameterId = Integer.parseInt(values[i++]);
//		}
//
//		for (int j = 0; j < 6; j++)
//		{
//			tgsm[j].GsmNcellCarrierRSSI = Integer.parseInt(values[i++]);
//			tgsm[j].GsmNcellBcch = Integer.parseInt(values[i++]);
//			tgsm[j].GsmNcellBsic = Integer.parseInt(values[i++]);
//		}
//		for (int j = 0; j < trip.length; j++)
//		{
//			trip[j].Earfcn = Integer.parseInt(values[i++]);
//			trip[j].SubFrame = Short.parseShort(values[i++]);
//			trip[j].LteScRIP = Short.parseShort(values[i++]);
//		}
//		LocFillType = Integer.parseInt(values[i++]);// 0是正常????是经纬度回填
//		testType = Integer.parseInt(values[i++]);
//		location = Integer.parseInt(values[i++]);
//		dist = Long.parseLong(values[i++]);
//		radius = Integer.parseInt(values[i++]);
//		loctp = values[i++];
//		indoor = Integer.parseInt(values[i++]);
//		networktype = values[i++];
//		lable = values[i++];// static,low,unknow,high
//		simuLongitude = Integer.parseInt(values[i++]);
//		simuLatitude = Integer.parseInt(values[i++]);
//		moveDirect = Integer.parseInt(values[i++]);
//		mrType = values[i++];
//		dfcnJamCellCount = Integer.parseInt(values[i++]);// 异频干扰小区个数
//		sfcnJamCellCount = Integer.parseInt(values[i++]);// 同频干扰小区个数
//
//		// zks add
//		LteScPUSCHPRBNum = Integer.parseInt(values[i++]);
//		LteScPDSCHPRBNum = Integer.parseInt(values[i++]);
//		LteSceNBRxTxTimeDiff = Integer.parseInt(values[i++]);
//	}

	public List<EventData> toEventData_Detail()
	{
		List<EventData> eventDatas = new ArrayList<>();
		// 以qci1判断是否故障事件
		if (MroErrorEventData.ErrorType.isErr(qciData.formatedULQci[0], qciData.formatedDLQci[0]))
		{
			MroErrorEventData errEvent = new MroErrorEventData();
			int i = 0;
			for (; i < qciData.formatedULQci.length; i++)
			{
				errEvent.eventDetial.fvalue[i] = qciData.formatedULQci[i];
			}
			for (int j = 0; j < qciData.formatedDLQci.length; j++, i++)
			{
				errEvent.eventDetial.fvalue[i] = qciData.formatedDLQci[j];
			}
			errEvent.eventDetial.strvalue[0] = MroErrorEventData.ErrorType.fromQCI_1(qciData.formatedULQci[0], qciData.formatedDLQci[0]).getName();

			errEvent.iCityID = cityID;
			errEvent.IMSI = IMSI;
			errEvent.iEci = Eci;
			errEvent.iTime = itime;
			errEvent.wTimems = wtimems;
			errEvent.strLoctp = locType;
			errEvent.strLabel = label;
			errEvent.iLongitude = ilongitude;
			errEvent.iLatitude = ilatitude;
			errEvent.confidentType = ConfidenceType;
			errEvent.lTrainKey = trainKey;
			errEvent.iSectionId = sectionId;
			errEvent.iSegmentId = segmentId;
			/* zhaikaishun 增加eventStat 统计 */
			if("单通".equals(errEvent.eventDetial.strvalue[0])){
				errEvent.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
				errEvent.eventStat.fvalue[18]=1;
			}else if("断续".equals(errEvent.eventDetial.strvalue[0])){
				errEvent.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
				errEvent.eventStat.fvalue[19]=1;
			}else{

			}
			if(errEvent.haveEventStat()){
				eventDatas.add(errEvent);
			}
		}
		return eventDatas;
	}

	public List<EventData> toEventData()
	{
	    if (!isQciData()) {
	        return Collections.emptyList();
        }
		List<EventData> eventDatas = new ArrayList<>();
		EventData eventData1 = new EventData();
		eventData1.iCityID = cityID;
		eventData1.IMSI = IMSI;
		eventData1.iBuildID = ibuildingID;
		eventData1.iHeight = iheight;
		eventData1.iEci = Eci;
		eventData1.strLoctp = locType;
		eventData1.strLabel = label;
		eventData1.iLongitude = ilongitude;
		eventData1.iLatitude = ilatitude;
		eventData1.iTestType = testType;
		// eventData1.iDoorType = samState;
		eventData1.iLocSource = locSource;
		eventData1.Interface = StaticConfig.DATA_TYPE_MR;
		eventData1.iKpiSet = StaticConfig.UL;// 上行
		eventData1.iTime = itime;
		eventData1.wTimems = wtimems;
		eventData1.gridItem = grid;
		eventData1.confidentType = ConfidenceType;
		eventData1.iAreaID = iAreaID;
		eventData1.iAreaType = iAreaType;
		eventData1.lTrainKey = trainKey;
		eventData1.iSectionId = sectionId;
		eventData1.iSegmentId = segmentId;
		eventData1.eventDetial = null;
		for (int i = 0; i < qciData.formatedULQci.length; i++)
		{
			eventData1.eventStat.fvalue[i * 2] = qciData.formatedULQci[i];
			if (eventData1.eventStat.fvalue[i * 2] > 0)
			{
				eventData1.eventStat.fvalue[i * 2 + 1] = 1D;
			}
			else
			{
				eventData1.eventStat.fvalue[i * 2 + 1] = StaticConfig.Double_Abnormal;
			}
		}

		if(eventData1.haveEventStat()){
			eventDatas.add(eventData1);
		}

		EventData eventData2 = new EventData();
		eventData2.iCityID = cityID;
		eventData2.IMSI = IMSI;
		eventData2.iBuildID = ibuildingID;
		eventData2.iHeight = iheight;
		eventData2.iEci = Eci;
		eventData2.strLoctp = locType;
		eventData2.strLabel = label;
		eventData2.iLongitude = ilongitude;
		eventData2.iLatitude = ilatitude;
		eventData2.iTestType = testType;
		// eventData2.iDoorType = samState;
		eventData2.iLocSource = locSource;
		eventData2.Interface = StaticConfig.DATA_TYPE_MR;
		eventData2.iKpiSet = StaticConfig.DL;// 下行
		eventData2.iTime = itime;
		eventData2.wTimems = wtimems;
		eventData2.gridItem = grid;
		eventData2.confidentType = ConfidenceType;
		eventData2.iAreaID = iAreaID;
		eventData2.iAreaType = iAreaType;
		eventData2.lTrainKey = trainKey;
		eventData2.iSectionId = sectionId;
		eventData2.iSegmentId = segmentId;
		eventData2.eventDetial = null;
		for (int i = 0; i < qciData.formatedDLQci.length; i++)
		{
			eventData2.eventStat.fvalue[i * 2] = qciData.formatedDLQci[i];
			if (eventData2.eventStat.fvalue[i * 2] >= 0)
			{
				eventData2.eventStat.fvalue[i * 2 + 1] = 1D;
			}
			else
			{
				eventData2.eventStat.fvalue[i * 2 + 1] = StaticConfig.Double_Abnormal;
			}
		}
		if(eventData2.haveEventStat()){
			eventDatas.add(eventData2);
		}

		//mod3
		EventData eventData3 = new EventData();
		eventData3.iCityID = cityID;
		eventData3.IMSI = IMSI;
		eventData3.iBuildID = ibuildingID;
		eventData3.iHeight = iheight;
		eventData3.iEci = Eci;
		eventData3.strLoctp = locType;
		eventData3.strLabel = label;
		eventData3.iLongitude = ilongitude;
		eventData3.iLatitude = ilatitude;
		eventData3.iTestType = testType;
		// eventData3.iDoorType = samState;
		eventData3.iLocSource = locSource;
		eventData3.Interface = StaticConfig.DATA_TYPE_MR;
		eventData3.iKpiSet = 3;
		eventData3.iTime = itime;
		eventData3.wTimems = wtimems;
		eventData3.gridItem = grid;
		eventData3.confidentType = ConfidenceType;
		eventData3.eventDetial = null;
		eventData3.iAreaID = iAreaID;
		eventData3.iAreaType = iAreaType;
		eventData3.lTrainKey = trainKey;
		eventData3.iSectionId = sectionId;
		eventData3.iSegmentId = segmentId;
		if (LteScPHR > -23)
		{
			eventData3.eventStat.fvalue[0] = 1D;
			eventData3.eventStat.fvalue[7] = LteScPHR;
			if (LteScPHR < -15)
			{
				eventData3.eventStat.fvalue[8] = 1D;
			}
			else if (LteScPHR < -13)
			{
				eventData3.eventStat.fvalue[9] = 1D;
			}
			else if (LteScPHR < -10)
			{
				eventData3.eventStat.fvalue[10] = 1D;
			}
			else if (LteScPHR < -6)
			{
				eventData3.eventStat.fvalue[11] = 1D;
			}
			else if (LteScPHR < 1)
			{
				eventData3.eventStat.fvalue[12] = 1D;
			}
			else
			{
				eventData3.eventStat.fvalue[13] = 1D;
			}
		}
		
		
		if (LteScPHR < 0 && LteScPHR > -23)
		{
			eventData3.eventStat.fvalue[1] = 1D;
		}
		if (LteScRIP > -126)
		{
			eventData3.eventStat.fvalue[2] = 1D;
		}
		if (LteScRIP > -105)
		{
			eventData3.eventStat.fvalue[3] = 1D;
		}
		eventData3.eventStat.fvalue[4] = 1D;
		for(NC_LTE nc_LTE : tlte)
		{
			if (nc_LTE.LteNcEarfcn == LteScEarfcn && (nc_LTE.LteNcRSRP-LteScRSRP > -6)
				&& (nc_LTE.LteNcPci-LteScPci)%3==0) {
				eventData3.eventStat.fvalue[5] = 1D;
				break;
			}
		}
		if (qciData.formatedULQci[1] > 0.2)
		{
			eventData3.eventStat.fvalue[6] = 1D;
		}
		if(eventData3.haveEventStat()){
			eventDatas.add(eventData3);
		}
		return eventDatas;
	}

	@Override
	public void fromFormatedString(String value) throws Exception {

		String[] strArr = value.split(spliter, -1);

		int index = 0;

		cityID = Integer.parseInt(strArr[index++]);

		itime = Integer.parseInt(strArr[index++]);

		wtimems = Short.parseShort(strArr[index++]);

		Eci = Long.parseLong(strArr[index++]);

		ibuildingID = Integer.parseInt(strArr[index++]);

		iheight = Short.parseShort(strArr[index++]);

		ilongitude = Integer.parseInt(strArr[index++]);

		ilatitude = Integer.parseInt(strArr[index++]);

		IMSI = Long.parseLong(Func.getDecrypt(strArr[index++]));

		MSISDN = Func.getDecrypt(strArr[index++]);

		MmeUeS1apId = Long.parseLong(strArr[index++]);

		xdrid = strArr[index++];

		UserLabel = strArr[index++];// wifilist

		imeiTac = Integer.parseInt(strArr[index++]);

		LteScRSRP = Integer.parseInt(strArr[index++]);

		LteScRSRQ = Integer.parseInt(strArr[index++]);

		LteScEarfcn = Integer.parseInt(strArr[index++]);

		LteScPci = Integer.parseInt(strArr[index++]);

		LteScBSR = Integer.parseInt(strArr[index++]);

		LteScRTTD = Integer.parseInt(strArr[index++]);

		LteScTadv = Integer.parseInt(strArr[index++]);

		LteScAOA = Integer.parseInt(strArr[index++]);

		LteScPHR = Integer.parseInt(strArr[index++]);

		LteScRIP = Double.parseDouble(strArr[index++]);

		LteScSinrUL = Integer.parseInt(strArr[index++]);

		for (int i = 0; i < nccount.length; i++)
		{
			nccount[i] = Short.parseShort(strArr[index++]);

		}
		for (NC_LTE nc_lte : tlte)
		{
			nc_lte.LteNcRSRP = Integer.parseInt(strArr[index++]);

			nc_lte.LteNcRSRQ = Integer.parseInt(strArr[index++]);

			nc_lte.LteNcEarfcn = Integer.parseInt(strArr[index++]);

			nc_lte.LteNcPci = Integer.parseInt(strArr[index++]);

			nc_lte.LteNcEci = Long.parseLong(strArr[index++]);

		}
		for (int i = 0; i < 3; i++)
		{
			tgsm[i].GsmNcellCarrierRSSI = Integer.parseInt(strArr[index++]);

			tgsm[i].GsmNcellBcch = Integer.parseInt(strArr[index++]);

			tgsm[i].GsmNcellBsic = Integer.parseInt(strArr[index++]);

			tgsm[i].GsmNcellLac = Integer.parseInt(strArr[index++]);

			tgsm[i].GsmNcellCi = Integer.parseInt(strArr[index++]);

		}

		for (NC_LTE nc_lte : dx_freq)
		{
			nc_lte.LteNcRSRP = Integer.parseInt(strArr[index++]);

			nc_lte.LteNcRSRQ = Integer.parseInt(strArr[index++]);

			nc_lte.LteNcEarfcn = Integer.parseInt(strArr[index++]);

			nc_lte.LteNcPci = Integer.parseInt(strArr[index++]);

			nc_lte.LteNcEci = Long.parseLong(strArr[index++]);

		}

		for (NC_LTE nc_lte : lt_freq)
		{
			nc_lte.LteNcRSRP = Integer.parseInt(strArr[index++]);

			nc_lte.LteNcRSRQ = Integer.parseInt(strArr[index++]);

			nc_lte.LteNcEarfcn = Integer.parseInt(strArr[index++]);

			nc_lte.LteNcPci = Integer.parseInt(strArr[index++]);

			nc_lte.LteNcEci = Long.parseLong(strArr[index++]);

		}

		overlapSameEarfcn = Integer.parseInt(strArr[index++]);

		OverlapAll = Integer.parseInt(strArr[index++]);

		mrType = strArr[index++];

		locType = strArr[index++];

		location = Integer.parseInt(strArr[index++]);

		moveDirect = Integer.parseInt(strArr[index++]);

		dist = Long.parseLong(strArr[index++]);

		radius = Integer.parseInt(strArr[index++]);

		label = strArr[index++];

	}

	public boolean isQciData() {
		return this.LteScRSRP == StaticConfig.Int_Abnormal;
	}

	@Override
	public String toFormatedString() {
		return createNewSampleToLine();
	}
}
