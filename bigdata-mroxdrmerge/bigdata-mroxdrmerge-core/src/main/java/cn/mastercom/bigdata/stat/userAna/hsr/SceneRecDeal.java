package cn.mastercom.bigdata.stat.userAna.hsr;



import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.mroxdrmerge.AppConfig;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userAna.model.HSRUserArea;
import cn.mastercom.bigdata.stat.userAna.model.HSRUserData;
import cn.mastercom.bigdata.stat.userAna.model.HSRUserXdr;
import cn.mastercom.bigdata.stat.userAna.tableEnums.HsrEnums;
import cn.mastercom.bigdata.util.IDataDeal;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;




public class SceneRecDeal implements IDataDeal{
	
	ResultOutputer resultOutputer; 
	Configuration conf;
	SceneRec sceneRec = new SceneRec();
	
	HSRUserData userData;
	
	SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
	
	public static long minsTime;
	public static long maxeTime;//最晚访问室分小区时间	
	public static long minEci;
	public static long maxEci;
	
//	private int count = 0;
	//List<HSRUserXdr> userXdrList;//存map读入的高铁用户
	Map<String, List<HSRUserArea>> userAreaMap;//key为imsi
	Set<HSRUserXdr> xdrSet;
	
	public SceneRecDeal(ResultOutputer resultOutputer){
			
		this.resultOutputer = resultOutputer;
		conf = MainModel.GetInstance().getConf();
		AppConfig appConfig = MainModel.GetInstance().getAppConfig();
		String hsrIndoorPath = appConfig.getHsrIndoorPath();
		sceneRec.init(conf, hsrIndoorPath);
		userData = new HSRUserData();

	}
			
	
	@Override
	public int pushData(int code, String value) {
		String[] strs = value.split("\t");
		
		HSRUserXdr xdrInfo = new HSRUserXdr();		
		xdrInfo.imsi = strs[0];
		xdrInfo.eci = Long.parseLong(strs[1]);
		xdrInfo.time = Long.parseLong(strs[2]);
		
		userData.userXdrList.add(xdrInfo);	
//		if(count==0) {
//			this.minsTime = xdrInfo.time;
//			this.minEci = xdrInfo.eci;
//			count = 1;
//		}else {
//			this.maxeTime = xdrInfo.time;
//			this.maxEci = xdrInfo.eci;
//		}
		xdrInfo = null;							
		return 0;
	}
	@Override
	public void statData() {
		
		if(userData.userXdrList == null || userData.userXdrList.isEmpty()){
			return;
		}		
		sceneRec.statData(userData);		
	}
	@Override
	public void outData() {
		
		for(HSRUserArea userArea : userData.userAreaList){
			try {
				StringBuilder sb = new StringBuilder();
				//吐出用户场景信息	
				if(!"".equals(userArea.imsi) &&userArea.imsi!=null){
				String[] imsiImei =  userArea.imsi.split("_");
					sb.append(imsiImei[0]).append("\t")
					.append(userArea.areaType.areaType).append("\t")
					.append(userArea.areaType.areaID).append("\t")
					.append(userArea.beginTime/1000).append("\t")
					.append(userArea.endTime/1000).append("\t")
//					.append(sdf.format(userArea.beginTime)).append("\t")
//					.append(sdf.format(userArea.endTime)).append("\t")
					.append(userArea.stationId).append("\t")
					.append(imsiImei.length > 1 ? imsiImei[1] : "");

					if(sb.length() > 0){					
						resultOutputer.pushData(HsrEnums.HSR_USER_AREA.getIndex(), sb.toString());
					}
				
				}
				
			}catch (Exception e){
				e.printStackTrace();
				LOGHelper.GetLogger().writeLog(LogType.error,"HSRUserArea DATA OUTPUT ERROR", "HSRUserArea DATA OUTPUT ERROR ", e);
			}
			
		}
		userData = new HSRUserData();

	}
	
}
