package cn.mastercom.bigdata.stat.userAna.hsr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.mroxdrmerge.AppConfig;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userAna.model.ImsiInfoSimple;
import cn.mastercom.bigdata.stat.userAna.model.StationImsi;
import cn.mastercom.bigdata.stat.userAna.tableEnums.HsrEnums;
import cn.mastercom.bigdata.util.IDataDeal;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader.LineHandler;

/**
 * @author Kwong
 * 高铁潜在用户计算
 */
public class HSRPotentialUserDeal implements IDataDeal{

//	private Map<Long, Integer> eciToStationid = new HashMap<>();
	private Map<String, StationImsi> stationImsis = new HashMap<>();
	List<RailSecImsi> railSecImsis = null;
	
	ResultOutputer resultOutputer;
	HSRUserAna hsrUserAna;
	HSRConfigHelper hsrConfigHelper;
	
	public HSRPotentialUserDeal(ResultOutputer resultOutputer) throws IOException, InterruptedException{
		this.resultOutputer = resultOutputer;
		
//		String hsrStationCellPath = MainModel.GetInstance().getAppConfig().getHsrStationCellPath();
		Configuration conf = MainModel.GetInstance().getConf();
//		initHSRFilter(hsrStationCellPath);
		hsrUserAna = new HSRUserAna();
		AppConfig appConfig = MainModel.GetInstance().getAppConfig();
		hsrUserAna.init(conf, appConfig.getHsrStationPath(), appConfig.getHsrSectionPath(), appConfig.getHsrStationCellPath(), appConfig.getHsrSectionCellPath(), appConfig.getHsrSectionRruPath(), appConfig.getHsrSegmentPath(), appConfig.getHsrIndoorPath());	
		hsrConfigHelper = HSRConfigHelper.getInstance();
	}
	
	@Override
	public int pushData(int dataType, String value) {
		
		StationImsi stationImsiTmp = StationImsi.fromString(value);
		
		if(stationImsiTmp.stationID <= 0){

			Set<RailStation> stationSet = null;
			if((stationSet = hsrConfigHelper.eci_station_map.get(stationImsiTmp.minTimeEci)) != null){
				
				//eci对应的所有站点 都统计 这条数据
				for(RailStation station : stationSet){
					
					stationImsiTmp.stationID = station.id;
					mergeStationImsi(stationImsiTmp);
				}
			}
			
		}else{
			mergeStationImsi(stationImsiTmp);
		}
		
		
		
		return 0;
	}
	
	private void mergeStationImsi(StationImsi stationImsiTmp){
		String stationImsiKey = stationImsiTmp.stationID + "\t" + stationImsiTmp.imsi;
		StationImsi stationImsi = stationImsis.get(stationImsiKey);
		if(stationImsi == null){
			stationImsis.put(stationImsiKey, new StationImsi(stationImsiTmp.stationID, stationImsiTmp.imsi, stationImsiTmp.minTime, stationImsiTmp.minTimeEci, stationImsiTmp.maxTime, stationImsiTmp.maxTimeEci));
		}else{
			stationImsi.merge(stationImsiTmp);
		}
	}

	@Override
	public void statData() {
		
		List<StationImsi> data = new ArrayList<>(stationImsis.values());
		railSecImsis = hsrUserAna.statData(data);			
	}

	@Override
	public void outData() {
		if(railSecImsis != null){
			for(RailSecImsi railSegImsi : railSecImsis){
				int sectionId = railSegImsi.railSecID;
				
				try
				{
					//one to two
					for(Iterator<ImsiInfoSimple> iterator = railSegImsi.imsiInfoSimpleMap1.values().iterator(); 
							iterator.hasNext();
							resultOutputer.pushData(HsrEnums.HSR_TMP_POTENTIAL_USER.getIndex(), sectionId + "\t" + RailSecImsi.ONE_TO_TWO + "\t" + iterator.next().toString()));
					//two to one
					for(Iterator<ImsiInfoSimple> iterator = railSegImsi.imsiInfoSimpleMap2.values().iterator(); 
							iterator.hasNext();
							resultOutputer.pushData(HsrEnums.HSR_TMP_POTENTIAL_USER.getIndex(), sectionId + "\t" + RailSecImsi.TWO_TO_ONE + "\t" + iterator.next().toString()));
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
		for(StationImsi stationImsi : stationImsis.values()){
			try {
				resultOutputer.pushData(0, stationImsi.toString());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/*private void initHSRFilter(String hsrStationCellPathStr) throws IOException, InterruptedException{
		try
		{
			//格式：cityid	站点id	ECI	Comment
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
	}*/
}
