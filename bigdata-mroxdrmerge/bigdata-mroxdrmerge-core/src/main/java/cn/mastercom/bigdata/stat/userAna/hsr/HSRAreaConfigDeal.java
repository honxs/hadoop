package cn.mastercom.bigdata.stat.userAna.hsr;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.stat.userAna.hsr.HSRSecTrainData.LocPoint;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader.LineHandler;





public class HSRAreaConfigDeal {

	private static HSRAreaConfigDeal areaConfigDeal = null;
	HSRSecTrainData secTrainData = null;
	public static HSRAreaConfigDeal getInstance() {
		
		if (areaConfigDeal == null) {
			areaConfigDeal = new HSRAreaConfigDeal();
		}
		return areaConfigDeal;
	}
	
	//eci-室分小区
	public Map<Long, Integer> eci_indoor_map;
	//job2吐出的imsi
	public Map<String, List<Long>> imsi_secMap;
	//job2吐出的车次信息
	public List<HSRSecTrainData> train_InfoList;
	//job2吐出的车次定位点(车次, point)
	public Map<Long, List<LocPoint>> sec_PoinMap;
	
	private HSRAreaConfigDeal() {		
		eci_indoor_map = new HashMap<Long, Integer>();
		imsi_secMap = new HashMap<String, List<Long>>();
		train_InfoList = new ArrayList<HSRSecTrainData>();
		sec_PoinMap = new HashMap<Long, List<LocPoint>>();
	}
	


	public void readConfig(Configuration conf, String hsrIndoorFilePath, String imsiPath, String trainInfoPath, String pointPath) 
					throws Exception {
		
		//hsrIndoorFilePath: path + "tb_高铁分析_配置_室分小区.txt" 
		FileReader.readFiles(conf, hsrIndoorFilePath, new LineHandler(){			
			@Override
			public void handle(String line){// CityID 站点ID ECI 
				String[] arrs = line.trim().split("\t");		
				int id = Integer.parseInt(arrs[1]);
				long eci = Long.parseLong(arrs[2]);
				eci_indoor_map.put(eci, id);			
			}
		});	
		/* job2的车次输出*/
		FileReader.readFiles(conf, trainInfoPath, new LineHandler() {
					@Override
					public void handle(String line)
					{
						String[] arrs = line.trim().split("\t");
						int cityID = Integer.parseInt(arrs[0]);
						long trainkey = Long.parseLong(arrs[1]);
						long lstime = Long.parseLong(arrs[5]) * 1000L;
						long letime = Long.parseLong(arrs[6]) * 1000L;
						int startStationid = Integer.parseInt(arrs[7]);
						int endStatiionid = Integer.parseInt(arrs[8]);
						boolean stopStart = Boolean.parseBoolean(arrs[10]);
						boolean stopEnd = Boolean.parseBoolean(arrs[11]);
						double length = Double.parseDouble(arrs[12]);
						secTrainData = new HSRSecTrainData(trainkey, cityID, lstime, letime,startStationid, 
								endStatiionid, length, stopStart, stopEnd);
						train_InfoList.add(secTrainData);
					}
				});
		/* job2的定位点输出*/
		secTrainData = new HSRSecTrainData();
		FileReader.readFiles(conf, pointPath, new LineHandler(){

			@Override
			public void handle(String line)
			{
				String[] strArr = line.trim().split("\t");
				long trainkey = Long.parseLong(strArr[0]);
				long ltime = Long.parseLong(strArr[1]) * 1000L;
			    double distance = Double.parseDouble(strArr[2]);//距离车次起始站的距离
				
				List<LocPoint> LocPointList = sec_PoinMap.get(trainkey);
				if(LocPointList == null ||LocPointList.size()<=0){
					LocPointList = new ArrayList<LocPoint>();
					sec_PoinMap.put(trainkey, LocPointList);
				}
				LocPointList.add(secTrainData.new LocPoint(ltime, distance));
			}
		});
		/* job2的imsi输出*/
		FileReader.readFiles(conf, imsiPath, new LineHandler(){

			@Override
			public void handle(String line)
			{
				String[] strArr = line.trim().split("\t");
				long trainkey = Long.parseLong(strArr[0]);
				String imsi = strArr[1] + "_";
				if(strArr.length >= 3) imsi = imsi + strArr[2];
					
				List<Long> trainList = imsi_secMap.get(imsi);
				if(trainList == null ||trainList.size()<=0){
					trainList = new ArrayList<Long>();
					imsi_secMap.put(imsi, trainList);
				}
				trainList.add(trainkey);
			}
		});
		
	}
	
	public boolean checkInit(){
		if(eci_indoor_map == null || eci_indoor_map.isEmpty())
			return false;
		if(imsi_secMap == null || imsi_secMap.isEmpty())
			return false;
		if(train_InfoList == null || train_InfoList.isEmpty())
			return false;
		return true;
	}
	
/*	public class ImsiSec{		
		public long trainkey;
		public long time;
	}*/
}

	



