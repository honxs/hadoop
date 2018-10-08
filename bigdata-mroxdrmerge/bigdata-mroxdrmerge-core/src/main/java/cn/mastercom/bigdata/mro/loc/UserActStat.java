package cn.mastercom.bigdata.mro.loc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;

public class UserActStat
{
	public long imsi;
	public String msisdn;
	
	public Map<Integer, UserActTime> userActTimeMap = new HashMap<Integer, UserActTime>();
	
	public UserActStat(long imsi, String msisdn)
	{
		this.imsi = imsi;
		this.msisdn = msisdn;
	}
	
	public void dealSample(DT_Sample_4G sample)
	{
		LteCellInfo lteCell = CellConfig.GetInstance().getLteCell(sample.Eci);
		if(lteCell == null)
		{
			return;
		}
		
		int tmTime = sample.itime/3600*3600;
		UserActTime userActTime = userActTimeMap.get(tmTime);
		if(userActTime == null)
		{
			userActTime = new UserActTime(tmTime, tmTime+3600);
			userActTimeMap.put(tmTime, userActTime);
		}
		userActTime.dealMro(sample);
	}
	
	public void finalStat()
	{
		for (UserActTime userActTime : userActTimeMap.values())
		{
			userActTime.finalStat();
		}
	}
	
	
	public class UserActTime
	{
		public int stime;
		public int etime;
		
		public Map<Long, UserCellAll> userCellAllMap;
		
		public UserActTime(int stime, int etime)
		{
		   this.stime = stime;
		   this.etime = etime;
		   
		   userCellAllMap = new HashMap<Long, UserCellAll>();
		}
		
		public void dealMro(DT_Sample_4G sample)
		{
			UserCellAll userCellAll = userCellAllMap.get(sample.Eci);
			if(userCellAll == null)
			{
				userCellAll = new UserCellAll(sample.Eci);
				userCellAllMap.put(sample.Eci, userCellAll);
			}
			userCellAll.dealSample(sample);			
		}
		
		public void finalStat()
		{
			for (UserCellAll userCellAll : userCellAllMap.values())
			{
				userCellAll.finalDeal();
			}
		}
	}
	
	public class UserCellAll
	{
        public long eci;	
		public Map<Long, UserCell> userCellMap = new HashMap<Long, UserCell>();
		public List<UserCell> userCellList = new ArrayList<UserCell>();
		
        public UserCellAll(long eci)
        {
        	this.eci = eci;
        }
        
		public void dealSample(DT_Sample_4G sample)
		{
			//对小区进行统计
			LteCellInfo lteCell = CellConfig.GetInstance().getLteCell(sample.Eci);
			
			UserCell userCell = userCellMap.get(sample.Eci);
			if(userCell == null)
			{
				userCell = new UserCell(sample.Eci);
				userCellMap.put(sample.Eci, userCell);
			}
			userCell.dealSample(sample, sample.LteScRSRP);
					
			for (NC_LTE item : sample.tlte)
			{
				if(item.LteNcRSRP > -150 && item.LteNcRSRP < 0)
				{
					LteCellInfo cellInfo = CellConfig.GetInstance().getNearestCell(lteCell.cityid, item.LteNcEarfcn, item.LteNcPci);
					if(cellInfo != null)
					{
						userCell = userCellMap.get(cellInfo.eci);
						if(userCell == null)
						{
							userCell = new UserCell(cellInfo.eci);
							userCellMap.put(cellInfo.eci, userCell);
						}
						userCell.dealSample(sample, item.LteNcRSRP);
					}
				}	
			}
			
		}
		
		public List<UserCell> getUserCellList()
		{
			return userCellList;
		}
		
		public UserCell getMainUserCell()
		{
			return userCellMap.get(eci);
		}
		
		public void finalDeal()
		{
			userCellList = new ArrayList<UserCell>(userCellMap.values());
			Collections.sort(userCellList,new Comparator<UserCell>(){  
	            @Override  
	            public int compare(UserCell a, UserCell b) {  
	                return b.rsrpTotal - a.rsrpTotal;  
	            }       
	        });
				
			for (UserCell userCell : userCellMap.values())
			{
				userCell.finalDeal();
			}
		}
		
		
	}
	
	
	public class UserCell
	{
		public long eci;
		public int rsrpSum;//和
		public int rsrpTotal;//点数
		public int rsrpMaxMark;
		public int rsrpMinMark;
		
		public List<Integer> rsrpList = new ArrayList<Integer>(); 
		
		public UserCell(long eci)
		{
		   this.eci = eci;
		   
		   rsrpSum = 0;
		   rsrpTotal = 0;
		   rsrpMaxMark = -1000000;
		   rsrpMinMark = -1000000;
		}
		
		public void dealSample(DT_Sample_4G sample, int rsrp)
		{
			if(sample.LteScRSRP >= -150 && sample.LteScRSRP <= -30)
			{
				rsrpSum += sample.LteScRSRP;
				rsrpTotal++;
				
				rsrpList.add(sample.LteScRSRP);
			}
		}
		
		public void finalDeal()
		{
			Collections.sort(rsrpList,new Comparator<Integer>(){  
	            @Override  
	            public int compare(Integer a, Integer b) {  
	                return a - b;  
	            }       
	        });
					
			if(rsrpList.size() >= 1)
			{
				rsrpMaxMark = rsrpList.get(rsrpList.size() >= 2? rsrpList.size()-2:0);
				rsrpMinMark = rsrpList.get(rsrpList.size() >= 2?1:0);
			}	
		}
		
	}
	
	
}
