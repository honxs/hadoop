package cn.mastercom.bigdata.mro.stat.struct;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.TimeHelper;

/**
 * 邻区室内栅格统计
 * 
 * @author xmr
 *
 */
public class Stat_In_Nb_CellGrid implements IStat_4G {
	
	public int iCityID;
	public int iBuildingID;
	public int iHeight;
	public int tllongitude;
	public int tllatitude;
	public int brlongitude;
	public int brlatitude;
	public Long iLteScEci;
	// public int iLteScSampleCnt;
	public Long iLteNcEci;
	public int iEarfcn;
	public int iPci;
	public int iTime;
	public int iMRCnt;
	public int iMRRSRQCnt;
	public float fRSRPValue;
	public float fRSRQValue;
	public int iMRCnt_95;
	public int iMRCnt_100;
	public int iMRCnt_103;
	public int iMRCnt_105;
	public int iMRCnt_110;
	public int iMRCnt_113;
	public int iMRCnt_128;
	public int iRSRQ_14;

	public float fRSRPMax = StaticConfig.Int_Abnormal;
	public float fRSRPMin = StaticConfig.Int_Abnormal;
	public float fRSRQMax = StaticConfig.Int_Abnormal;
	public float fRSRQMin = StaticConfig.Int_Abnormal;
	public float fSINRMax = StaticConfig.Int_Abnormal;
	public float fSINRMin = StaticConfig.Int_Abnormal;
	public int i;
	HashMap<Long, Integer> eciCntMap = new HashMap<Long, Integer>();
	public static final String spliter = "\t";

	@Override
	public String toLine() {
		Long cntMaxEci = 0L;
		int eciSampleCnt = 0;
		for (Map.Entry<Long, Integer> eci : eciCntMap.entrySet()) {
			if (eci.getValue() > eciSampleCnt) {
				eciSampleCnt = eci.getValue();
				cntMaxEci = eci.getKey();
			}
		}
		iLteScEci = cntMaxEci;
	//	iLteScSampleCnt = eciSampleCnt;
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(iBuildingID);
		bf.append(spliter);
		bf.append(iHeight);
		bf.append(spliter);
		bf.append(tllongitude);
		bf.append(spliter);
		bf.append(tllatitude);
		bf.append(spliter);
		bf.append(brlongitude);
		bf.append(spliter);
		bf.append(brlatitude);
		bf.append(spliter);
		/*bf.append(iLteScSampleCnt);
		bf.append(spliter);*/
		bf.append(iLteNcEci);
		bf.append(spliter);
		bf.append(iEarfcn);
		bf.append(spliter);
		bf.append(iPci);
		bf.append(spliter);
		bf.append(iTime);
		bf.append(spliter);
		bf.append(iLteScEci);
		bf.append(spliter);
		bf.append(iMRCnt);
		bf.append(spliter);
		bf.append(iMRRSRQCnt);
		bf.append(spliter);
		bf.append(fRSRPValue);
		bf.append(spliter);
		bf.append(fRSRQValue);
		bf.append(spliter);
		bf.append(iMRCnt_95);
		bf.append(spliter);
		bf.append(iMRCnt_100);
		bf.append(spliter);
		bf.append(iMRCnt_103);
		bf.append(spliter);
		bf.append(iMRCnt_105);
		bf.append(spliter);
		bf.append(iMRCnt_110);
		bf.append(spliter);
		bf.append(iMRCnt_113);
		bf.append(spliter);
		bf.append(iMRCnt_128);
		bf.append(spliter);
		bf.append(iRSRQ_14);
		bf.append(spliter);
		bf.append(fRSRPMax);
		bf.append(spliter);
		bf.append(fRSRPMin);
		bf.append(spliter);
		bf.append(fRSRQMax);
		bf.append(spliter);
		bf.append(fRSRQMin);
		return bf.toString();
	}

	public void doFirstSample(DT_Sample_4G sample) {
		iCityID = sample.cityID;
		iBuildingID = sample.ibuildingID;
		iHeight = sample.iheight;
		tllongitude = sample.grid.tllongitude;
		tllatitude = sample.grid.tllatitude;
		brlongitude = sample.grid.brlongitude;
		brlatitude = sample.grid.brlatitude;
		iLteNcEci = sample.tlte[0].LteNcEci;
		iPci = sample.tlte[0].LteNcPci;
		iEarfcn = sample.tlte[0].LteNcEarfcn;
		iTime = FormatTime.RoundTimeForHour(sample.itime);
	}
	public void doFirstSample(DT_Sample_4G sample, int i) {
		this.i = i;
		iCityID = sample.cityID;
		iBuildingID = sample.ibuildingID;
		iHeight = sample.iheight;
		tllongitude = sample.grid.tllongitude;
		tllatitude = sample.grid.tllatitude;
		brlongitude = sample.grid.brlongitude;
		brlatitude = sample.grid.brlatitude;
		iLteNcEci = sample.tlte[i].LteNcEci;
		iPci = sample.tlte[i].LteNcPci;
		iEarfcn = sample.tlte[i].LteNcEarfcn;
		iTime = FormatTime.RoundTimeForHour(sample.itime);
	}

	@Override
	public void doSample(DT_Sample_4G sample) {
		if (!(sample.tlte[i].LteNcRSRP >= -150 && sample.tlte[i].LteNcRSRP <= -30))
			return;
		
		if (eciCntMap.containsKey(sample.Eci)) {
			int eciSampleCnt = eciCntMap.get(sample.Eci);
			eciSampleCnt++;
			eciCntMap.put(sample.Eci, eciSampleCnt);
		} else {
			eciCntMap.put(sample.Eci, 1);
		}
		iMRCnt++;
		fRSRPValue += sample.tlte[i].LteNcRSRP;

		if (sample.tlte[i].LteNcRSRQ != -1000000) {
			iMRRSRQCnt++;
			fRSRQValue += sample.tlte[i].LteNcRSRQ;
		}

		if (sample.tlte[i].LteNcRSRP >= -95) {
			iMRCnt_95++;
		}
		if (sample.tlte[i].LteNcRSRP >= -100) {
			iMRCnt_100++;
		}
		if (sample.tlte[i].LteNcRSRP >= -103) {
			iMRCnt_103++;
		}
		if (sample.tlte[i].LteNcRSRP >= -105) {
			iMRCnt_105++;
		}
		if (sample.tlte[i].LteNcRSRP > -110) {
			iMRCnt_110++;
		}
		if (sample.tlte[i].LteNcRSRP >= -113) {
			iMRCnt_113++;
		}
		if (sample.tlte[i].LteNcRSRP >= -128) {
			iMRCnt_128++;
		}

		if (sample.tlte[i].LteNcRSRQ >= -14) {
			iRSRQ_14++;
		}

		fRSRPMax = getMax(fRSRPMax, sample.tlte[i].LteNcRSRP);
		fRSRPMin = getMin(fRSRPMin, sample.tlte[i].LteNcRSRP);
		fRSRQMax = getMax(fRSRQMax, sample.tlte[i].LteNcRSRQ);
		fRSRQMin = getMin(fRSRQMin, sample.tlte[i].LteNcRSRQ);
	}

	private float getMax(float valueMax, int value) {
		if (valueMax == StaticConfig.Int_Abnormal || valueMax < value) {
			return value;
		}
		return valueMax;
	}

	private float getMin(float valueMin, int value) {
		if (value == StaticConfig.Int_Abnormal)
			return valueMin;
		if (valueMin == StaticConfig.Int_Abnormal || valueMin > value) {
			return value;
		}
		return valueMin;
	}

	public static void main(String args[]) {
		int a = -1000000;
		float b = -1000000f;
		System.out.println(b == a);

	}

	public static Stat_In_Nb_CellGrid FillData(String[] vals, int pos) {
		int i = pos;
		Stat_In_Nb_CellGrid nbIncellGrid = new Stat_In_Nb_CellGrid();
		try {
			nbIncellGrid.iCityID = Integer.parseInt(vals[i++]);
			nbIncellGrid.iBuildingID = Integer.parseInt(vals[i++]);
			nbIncellGrid.iHeight = Integer.parseInt(vals[i++]);
			nbIncellGrid.tllongitude = Integer.parseInt(vals[i++]);
			nbIncellGrid.tllatitude = Integer.parseInt(vals[i++]);
			nbIncellGrid.brlongitude = Integer.parseInt(vals[i++]);
			nbIncellGrid.brlatitude = Integer.parseInt(vals[i++]);			
			// nbIncellGrid.iLteScSampleCnt = Integer.parseInt(vals[i++]);
			nbIncellGrid.iLteNcEci = Long.parseLong(vals[i++]);
			nbIncellGrid.iEarfcn = Integer.parseInt(vals[i++]);
			nbIncellGrid.iPci = Integer.parseInt(vals[i++]);
			nbIncellGrid.iTime = TimeHelper.getRoundDayTime(Integer.parseInt(vals[i++]));
			nbIncellGrid.iLteScEci = Long.parseLong(vals[i++]);
			nbIncellGrid.iMRCnt = Integer.parseInt(vals[i++]);
			nbIncellGrid.iMRRSRQCnt = Integer.parseInt(vals[i++]);
			nbIncellGrid.fRSRPValue = Float.parseFloat(vals[i++]);
			nbIncellGrid.fRSRQValue = Float.parseFloat(vals[i++]);
			nbIncellGrid.iMRCnt_95 = Integer.parseInt(vals[i++]);
			nbIncellGrid.iMRCnt_100 = Integer.parseInt(vals[i++]);
			nbIncellGrid.iMRCnt_103 = Integer.parseInt(vals[i++]);
			nbIncellGrid.iMRCnt_105 = Integer.parseInt(vals[i++]);
			nbIncellGrid.iMRCnt_110 = Integer.parseInt(vals[i++]);
			nbIncellGrid.iMRCnt_113 = Integer.parseInt(vals[i++]);
			nbIncellGrid.iMRCnt_128 = Integer.parseInt(vals[i++]);
			nbIncellGrid.iRSRQ_14 = Integer.parseInt(vals[i++]);
			nbIncellGrid.fRSRPMax = Float.parseFloat(vals[i++]);
			nbIncellGrid.fRSRPMin = Float.parseFloat(vals[i++]);
			nbIncellGrid.fRSRQMax = Float.parseFloat(vals[i++]);
			nbIncellGrid.fRSRQMin = Float.parseFloat(vals[i++]);
		} catch (Exception e) {
			LOGHelper.GetLogger().writeLog(LogType.error, "Stat_In_CellGrid filldata error","", e);
		}
		return nbIncellGrid;
	}

	@Override
	public void doSampleLT(DT_Sample_4G sample) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSampleDX(DT_Sample_4G sample) {
		// TODO Auto-generated method stub

	}
}