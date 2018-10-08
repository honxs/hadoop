package cn.mastercom.bigdata.mro.stat.struct;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.TimeHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

import java.util.HashMap;
import java.util.Map;

/**
 * 邻区室外栅格统计
 * 
 * @author xmr
 *
 */
public class Stat_Out_Nb_CellGrid extends Stat_OutGrid {
	public Long iLteScEci;
	// public int iLteScSampleCnt;
	public Long iLteNcEci;
	public int iPci;
	public int iEarfcn;
	public static final String spliter = "\t";
	private int i;
	HashMap<Long, Integer> eciCntMap = new HashMap<Long, Integer>();

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
		// iLteScSampleCnt = eciSampleCnt;
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(tllongitude);
		bf.append(spliter);
		bf.append(tllatitude);
		bf.append(spliter);
		bf.append(brlongitude);
		bf.append(spliter);
		bf.append(brlatitude);
		bf.append(spliter);
	/*	bf.append(iLteScSampleCnt);
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
		tllongitude = sample.grid.tllongitude;
		tllatitude = sample.grid.tllatitude;
		brlongitude = sample.grid.brlongitude;
		brlatitude = sample.grid.brlatitude;
		iLteNcEci = sample.tlte[0].LteNcEci;
		iPci = sample.tlte[0].LteNcPci;
		iEarfcn = sample.tlte[0].LteNcEarfcn;
		iTime = FormatTime.RoundTimeForHour(sample.itime);
	}
	public void doFirstSample(DT_Sample_4G sample,int i) {
		this.i = i;
		iCityID = sample.cityID;
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

	public static Stat_Out_Nb_CellGrid FillData(String[] vals, int pos) {
		int i = pos;
		Stat_Out_Nb_CellGrid nbOutcellGrid = new Stat_Out_Nb_CellGrid();
		try {
			nbOutcellGrid.iCityID = Integer.parseInt(vals[i++]);
			nbOutcellGrid.tllongitude = Integer.parseInt(vals[i++]);
			nbOutcellGrid.tllatitude = Integer.parseInt(vals[i++]);
			nbOutcellGrid.brlongitude = Integer.parseInt(vals[i++]);
			nbOutcellGrid.brlatitude = Integer.parseInt(vals[i++]);
			//nbOutcellGrid.iLteScSampleCnt = Integer.parseInt(vals[i++]);
			nbOutcellGrid.iLteNcEci = Long.parseLong(vals[i++]);
			nbOutcellGrid.iPci = Integer.parseInt(vals[i++]);
			nbOutcellGrid.iEarfcn = Integer.parseInt(vals[i++]);
			nbOutcellGrid.iTime = TimeHelper.getRoundDayTime(Integer.parseInt(vals[i++]));
			nbOutcellGrid.iLteScEci = Long.parseLong(vals[i++]);
			nbOutcellGrid.iMRCnt = Integer.parseInt(vals[i++]);
			nbOutcellGrid.iMRRSRQCnt = Integer.parseInt(vals[i++]);
			nbOutcellGrid.fRSRPValue = Double.parseDouble(vals[i++]);
			nbOutcellGrid.fRSRQValue = Double.parseDouble(vals[i++]);
			nbOutcellGrid.iMRCnt_95 = Integer.parseInt(vals[i++]);
			nbOutcellGrid.iMRCnt_100 = Integer.parseInt(vals[i++]);
			nbOutcellGrid.iMRCnt_103 = Integer.parseInt(vals[i++]);
			nbOutcellGrid.iMRCnt_105 = Integer.parseInt(vals[i++]);
			nbOutcellGrid.iMRCnt_110 = Integer.parseInt(vals[i++]);
			nbOutcellGrid.iMRCnt_113 = Integer.parseInt(vals[i++]);
			nbOutcellGrid.iMRCnt_128 = Integer.parseInt(vals[i++]);
			nbOutcellGrid.iRSRQ_14 = Integer.parseInt(vals[i++]);
			nbOutcellGrid.fRSRPMax = Float.parseFloat(vals[i++]);
			nbOutcellGrid.fRSRPMin = Float.parseFloat(vals[i++]);
			nbOutcellGrid.fRSRQMax = Float.parseFloat(vals[i++]);
			nbOutcellGrid.fRSRQMin = Float.parseFloat(vals[i++]);
		} catch (Exception e) {
			LOGHelper.GetLogger().writeLog(LogType.error,"Stat_Out_Nb_CellGrid filldata error", "", e);
		}

		return nbOutcellGrid;
	}
}
