package cn.mastercom.bigdata.locsimu;

import java.util.HashMap;
import java.util.Map;

public class CellSimuMng
{
    public Map<String, Map<Long, CellSimuItem>> locationCellInfo = new HashMap<String, Map<Long, CellSimuItem>>();
    
    public boolean simuCellInfo(CellSimuItem item)
    {
    	String locKey = MakeLocKey(item.longitude, item.latitude);
    	
    	Map<Long, CellSimuItem> cellSimuMap = locationCellInfo.get(locKey);
    	if(cellSimuMap == null)
    	{
    		cellSimuMap = new HashMap<Long, CellSimuItem>();
    		locationCellInfo.put(locKey, cellSimuMap);
    	}
    	
    	CellSimuItem simuItem = cellSimuMap.get(item.eci);
    	if(simuItem == null)
    	{
    		simuItem = item;
    		cellSimuMap.put(item.eci, simuItem);
    	}
    	else 
    	{
    		simuItem.maxRsrp = Math.max(simuItem.maxRsrp, item.maxRsrp);
    		simuItem.maxRsrp = Math.max(simuItem.maxRsrp, item.maxRsrp);
    	}
    	
    	return true;
    }
    
    public static String MakeLocKey(int longitude, int latitude)
    {
    	return longitude + "," + latitude;
    }
	
//    public boolean loadData() 
//    {
//    	try
//		{
//    	 	HDFSOper hdfsOper = new HDFSOper("10.139.6.169", 9000);
//    	 	String filePath = "/mt_wlyh/Data/config/tb_cellsimu_info.txt";
//    	 	if(!hdfsOper.checkFileExist(filePath))
//    	 	{
//    	 		return false;
//    	 	}
//        	
//			BufferedReader reader = null;
//			String locKey = "";
//			Map<Long, CellSimuItem> cellSimuMap = locationCellInfo.get(locKey);
//			try
//			{
//				reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
//				String strData;
//				String[] values;
//				long eci;
//				long fcnPciKey;
//				List<LteCellInfo> fcnPciList = null;
//				while((strData = reader.readLine()) != null)
//				{
//					if(strData.length() == 0)
//					{
//						continue;
//					}
//					
//					try
//					{
//						values = strData.split("\t", -1);
//						LteCellInfo item = LteCellInfo.FillData(values);
//						if(item.enbid > 0 && item.cellid > 0)
//						{
//							eci = item.enbid * 256 + item.cellid;
//							lteCellInfoMap.put(eci, item);
//						}
//						
//						if(item.pci > 0 && item.fcn > 0 && item.cityid > 0)
//						{
//							fcnPciKey = Long.parseLong(String.format("%02d%05d%03d", item.cityid, item.fcn, item.pci));
//							fcnPciList = fcnPciLteCellMap.get(fcnPciKey);
//							if(fcnPciList == null)
//							{
//								fcnPciList = new ArrayList<LteCellInfo>();
//								fcnPciLteCellMap.put(fcnPciKey, fcnPciList);
//							}
//							fcnPciList.add(item);
//						}
//					}
//					catch (Exception e)
//					{
//						LOGHelper.GetLogger().writeLog(LogType.error, "loadLteCell error : " + strData, e);
//						return false;
//					}		
//				}
//			}
//			catch(Exception e)
//			{
//				LOGHelper.GetLogger().writeLog(LogType.error, "loadLteCell error ", e);
//				return false;
//			}
//			finally
//			{
//				if (reader != null)
//				{
//					reader.close();
//				}
//			}
//    	 	
//		}
//		catch (Exception e)
//		{
//			return false;
//		}
//   	
//    	return true;
//    }
//	
}
