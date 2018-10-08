package cn.mastercom.bigdata.xdr.loc;

import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.conf.cellconfig.LteCellConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

public class MrInGridCellNc implements IResultTable
{
	public int iCityID = 0;
	public int iBuildingID = 0;
	public int iHeight = 0;
	public int iLongitude = 0;
	public int iLatitude = 0;
	public int iECI = 0;
	public int iEarfcn = 0;
	public int iPci = 0;
	public int iTime = 0;
	public int iASNei_MRCnt = 0;
	public double fASNei_RSRPValue = 0;
	
	public MrInGridCellNc(MrInGridCell item, NC_LTE nclte)
	{
		iCityID = item.iCityID;
		iBuildingID = item.iBuildingID;
		iHeight = item.iHeight;
		iLongitude = item.iLongitude;
		iLatitude = item.iLatitude;
		iECI = item.iECI;
		iEarfcn = nclte.LteNcEarfcn;
		iPci = nclte.LteNcPci;
		iTime = item.iTime;
	}
	
	public static final String TypeName = "mringridcellnc";
	
	public static String getKey(MrInGridCell item, NC_LTE nclte)
	{
		//此处按天计算,所以没有添加时间因素
		return item.iCityID 
				+ "_" + item.iBuildingID
				+ "_" + item.iHeight
				+ "_" + item.iLongitude
				+ "_" + item.iLatitude
				+ "_" + item.iECI
				+ "_" + nclte.LteNcEarfcn
				+ "_" + nclte.LteNcPci;
	}
	
	public void Stat(NC_LTE nclte)
	{
		if (nclte.LteNcRSRP != -1000000)
		{
			iASNei_MRCnt++;
			fASNei_RSRPValue += nclte.LteNcRSRP;
		}
	}
	
    
	public String toLine()
	{
		StringBuffer sb = new StringBuffer();
		String tabMark = ResultHelper.TabMark;
		
		sb.append(iCityID);
		sb.append(tabMark);sb.append(iBuildingID);
		sb.append(tabMark);sb.append(iHeight);
		sb.append(tabMark);sb.append(iLongitude);
		sb.append(tabMark);sb.append(iLatitude);
		sb.append(tabMark);sb.append(iECI);
		sb.append(tabMark);sb.append(getNbEci());
		sb.append(tabMark);sb.append(iEarfcn);
		sb.append(tabMark);sb.append(iPci);
		sb.append(tabMark);sb.append(iTime);
		sb.append(tabMark);sb.append(iASNei_MRCnt);
		sb.append(tabMark);sb.append(fASNei_RSRPValue);

		return sb.toString();
	}
	
	public long getNbEci()
	{
		LteCellInfo nbCell = null;
		try
		{
			nbCell = LteCellConfig.GetInstance().getNearestCell(iECI, iEarfcn, iPci);
			if(nbCell != null){

			}return nbCell.eci;
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc MrInGridCellNc.getNbEci error",
					"xdrloc MrInGridCellNc.getNbEci error",	e);
		}
		return -1;
	}

}
