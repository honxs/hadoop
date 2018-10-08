package cn.mastercom.bigdata.locsimu;

import java.util.HashMap;
import java.util.HashSet;

import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.StructData.DT_Sample_4G;

public class CellSimuDeal
{
	private CellSimuMng cellSimuMng = new CellSimuMng();
	private HashMap<Long, HashSet<String>> mainCellLocation = new HashMap<Long, HashSet<String>>();// HashMap<小区id,


	public void addSample(DT_Sample_4G sample ) 
	{

		if (sample.ilongitude == 0 
		  || sample.ilatitude == 0
		  || sample.LteScRSRP < -150
		  || sample.LteScRSRP > 0)
		{
			return;
		}
		
		LteCellInfo cellInfo = CellConfig.GetInstance().getLteCell(sample.Eci);
		if(cellInfo == null)
		{
			return;
		}
		
		CellSimuItem cellSimuItem = new CellSimuItem();
		cellSimuItem.eci = sample.iCI;
		cellSimuItem.longitude = sample.ilongitude;
		cellSimuItem.latitude = sample.ilatitude;
		cellSimuItem.cellLongitude = cellInfo.ilongitude;
		cellSimuItem.cellLatitude = cellInfo.ilatitude;
		cellSimuItem.maxRsrp = sample.LteScRSRP;
		cellSimuItem.minRsrp = sample.LteScRSRP;
		
		cellSimuMng.simuCellInfo(cellSimuItem);
		
		for(int i = 0; i<sample.tlte.length; ++i)
		{
			cellSimuItem = new CellSimuItem();
			
			if(sample.tlte[i].LteNcEarfcn < 0 
			  || sample.tlte[i].LteNcPci < 0
			  || (sample.tlte[i].LteNcRSRP < -150 || sample.tlte[i].LteNcRSRP > 0 ))
			{
				break;
			}
			
//			LteCellInfo nbCellInfo = CellConfig.GetInstance().getNearestCell(sample.ilongitude, sample.ilatitude, sample.cityID, sample.tlte[i].LteNcEarfcn, sample.tlte[i].LteNcPci);
//			if(nbCellInfo == null)
//			{
//				continue;
//			}
//			
//			cellSimuItem.eci = nbCellInfo.cellid;
//			cellSimuItem.longitude = sample.ilongitude;
//			cellSimuItem.latitude = sample.ilatitude;
//			cellSimuItem.cellLongitude = nbCellInfo.ilongitude;
//			cellSimuItem.cellLatitude = nbCellInfo.ilatitude;
//			cellSimuItem.maxRsrp = sample.LteScRSRP;
//			cellSimuItem.minRsrp = sample.LteScRSRP;	
			
			cellSimuMng.simuCellInfo(cellSimuItem);
		}
		
		String locStr = CellSimuMng.MakeLocKey(sample.ilongitude, sample.ilatitude);
		HashSet<String> locSet = mainCellLocation.get(sample.Eci);
		if(locSet == null)
		{
			locSet = new HashSet<String>();
			mainCellLocation.put(sample.Eci, locSet);
		}
		locSet.add(locStr);
		
	}
}
