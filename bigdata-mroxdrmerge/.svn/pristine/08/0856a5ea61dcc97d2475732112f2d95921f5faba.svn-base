package cn.mastercom.bigdata.stat.userAna.subway;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.mroxdrmerge.AppConfig;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userAna.hsr.HSRUserAna;
import cn.mastercom.bigdata.stat.userAna.hsr.RailSecImsi;
import cn.mastercom.bigdata.stat.userAna.model.ImsiInfoSimple;
import cn.mastercom.bigdata.stat.userAna.model.StationImsi;
import cn.mastercom.bigdata.stat.userAna.tableEnums.SubwayEnums;
import cn.mastercom.bigdata.util.IDataDeal;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader.LineHandler;

public class SubwayPotentialUserDeal implements IDataDeal{

	private static final long STAY_TIME_SPAN_MAX = 3600 * 1000L;
	
	private Map<Long, Integer> eciToStationid = new HashMap<>();
	private Map<String, List<StationImsi>> stationImsiGroups = new HashMap<>();
	List<RailSecImsi> railSecImsis = null;
	
	Configuration conf;
	ResultOutputer resultOutputer;
	
	public SubwayPotentialUserDeal(ResultOutputer resultOutputer) throws IOException, InterruptedException{
		this.resultOutputer = resultOutputer;
		
		String hsrStationCellPath = MainModel.GetInstance().getAppConfig().getHsrStationCellPath();
		conf = MainModel.GetInstance().getConf();
		initSubwayFilter(hsrStationCellPath);
	}
	
	@Override
	public int pushData(int dataType, String value) {
		StationImsi stationImsiTmp = StationImsi.fromString(value);
		
		if(stationImsiTmp.stationID <= 0){			
			Integer stationId = eciToStationid.get(stationImsiTmp.minTimeEci);
			if(stationId == null){
				return -1;
			}
			stationImsiTmp.stationID = stationId;
		}
		
		String stationImsiKey = stationImsiTmp.stationID + "\t" + stationImsiTmp.imsi;
		List<StationImsi> stationImsiGroup = stationImsiGroups.get(stationImsiKey);
		if(stationImsiGroup == null){
			stationImsiGroup = new ArrayList<>();
			stationImsiGroup.add(stationImsiTmp);
			stationImsiGroups.put(stationImsiKey, stationImsiGroup);
		}else{
			boolean newGroup = true;
			for(StationImsi stationImsi : stationImsiGroup){				
				if(Math.abs(stationImsiTmp.minTime - stationImsi.minTime) < STAY_TIME_SPAN_MAX || Math.abs(stationImsiTmp.maxTime - stationImsi.maxTime) < STAY_TIME_SPAN_MAX)
				{
					stationImsi.merge(stationImsiTmp);
					newGroup = false;
					break;
				}
			}
			if(newGroup){
				stationImsiGroup.add(stationImsiTmp);
			}
		}
		
		return 0;
	}

	@Override
	public void statData() {
		
		HSRUserAna subwayUserAna = new HSRUserAna();
		AppConfig appConfig = MainModel.GetInstance().getAppConfig();
		subwayUserAna.init(conf, appConfig.getHsrStationPath(), appConfig.getHsrSectionPath(), appConfig.getHsrStationCellPath(), appConfig.getHsrSectionCellPath(), appConfig.getHsrSectionRruPath(), appConfig.getHsrSegmentPath(), appConfig.getHsrIndoorPath());
		
		List<StationImsi> data = new ArrayList<>();
		for(List<StationImsi> stationImsiGroup : stationImsiGroups.values()){
			data.addAll(stationImsiGroup);
		}
		railSecImsis = subwayUserAna.statData(data);		
	}

	@Override
	public void outData() {
		if(railSecImsis != null){
			for(RailSecImsi railSecImsi : railSecImsis){
				int sectionId = railSecImsi.railSecID;
				
				try
				{
					//one to two
					for(Iterator<ImsiInfoSimple> iterator = railSecImsi.imsiInfoSimpleMap1.values().iterator(); 
							iterator.hasNext();
							resultOutputer.pushData(SubwayEnums.SUBWAY_TMP_POTENTIAL_USER.getIndex(), sectionId + "\t" + RailSecImsi.ONE_TO_TWO + "\t" + iterator.next().toString()));
					//two to one
					for(Iterator<ImsiInfoSimple> iterator = railSecImsi.imsiInfoSimpleMap2.values().iterator(); 
							iterator.hasNext();
							resultOutputer.pushData(SubwayEnums.SUBWAY_TMP_POTENTIAL_USER.getIndex(), sectionId + "\t" + RailSecImsi.TWO_TO_ONE + "\t" + iterator.next().toString()));
				}
				catch (Exception e)
				{
					e.printStackTrace();
					throw new RuntimeException(e);
				}
				
			}
			
		}else{//输出map统计 
			outMapData();
		}
	}

	private void outMapData(){
		for(List<StationImsi> stationImsiGroup : stationImsiGroups.values()){
			for(StationImsi stationImsi : stationImsiGroup){
				
				try {
					resultOutputer.pushData(0, stationImsi.toString());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	private void initSubwayFilter(String hsrStationCellPathStr) throws IOException, InterruptedException{
		try
		{
			//格式：cityid	站点id	ECI	Comment
			//TODO 暂时不考虑一个小区覆盖两个站点的情况
			FileReader.readFile(conf, hsrStationCellPathStr, new LineHandler(){

				@Override
				public void handle(String line)
				{
					String[] strArr = line.split("\t");
					eciToStationid.put(Long.parseLong(strArr[2]), Integer.parseInt(strArr[1]));
				
				}
			});
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new InterruptedException("初始化高铁小区配置异常");
		}
	}
}
