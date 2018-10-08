package cn.mastercom.bigdata.StructData;

import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.conf.cellconfig.TdCellInfo;
import cn.mastercom.bigdata.util.GisFunction;

public class SIGNAL_XDR_3G extends SIGNAL_LOC
{

	public int eventid;
	public int eventType;
	public int lac;
	public int ci;

	public int nettype;//1 gsm;2 tdscdma;3 lte
	public String uenettype;
	public String timeStr;
	public int lockNetMark;
	
	public SIGNAL_XDR_3G()
	{
		Clear();
	}

	@Override
	public String GetCellKey()
	{
		return lac + "_" + ci;
	}

	@Override
	public int GetSampleDistance(int ilongitude, int ilatitude)
	{		
		TdCellInfo cellInfo = CellConfig.GetInstance().getTdCell(lac, ci);	
		if(cellInfo != null)
		{
			if(longitude > 0 && latitude > 0 && cellInfo.ilongitude > 0 && cellInfo.ilatitude > 0)
			{
				return (int)GisFunction.GetDistance(ilongitude, ilatitude, cellInfo.ilongitude, cellInfo.ilatitude);	
			}	
		}
		
		return StaticConfig.Int_Abnormal;
	}

	@Override
	public int GetMaxCellRadius()
	{
		int maxRadius = 6000;
		TdCellInfo cellInfo = CellConfig.GetInstance().getTdCell(lac, ci);	
		if(cellInfo != null)
		{
			maxRadius = Math.min(maxRadius, 5*cellInfo.radius);	
			maxRadius = Math.max(maxRadius, 1500);
		}
			
		return maxRadius;
	}

}
