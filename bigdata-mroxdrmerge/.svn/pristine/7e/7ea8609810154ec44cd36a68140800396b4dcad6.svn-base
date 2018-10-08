package cn.mastercom.bigdata.stat.userAna.hsr;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.stat.userAna.model.HSRUserData;
import cn.mastercom.bigdata.stat.userAna.tableEnums.HsrEnums;




public class SceneRec {
	
	//初始化室分小区配置
	public int init(Configuration conf, String hsrIndoorPath) {
		try {
			
			String baseOutputPath = conf.get("mapreduce.job.output.path");
			String date = conf.get("mapreduce.job.date");
			String imsipath = HsrEnums.HSR_IMSI.getPath(baseOutputPath, date);//获取用户的区间
			String trainInfoPath = HsrEnums.HSR_TRAIN_INFO.getPath(baseOutputPath, date);//获取车次信息
			String pointInfoPath = HsrEnums.HSR_LOCATION_POINT.getPath(baseOutputPath, date);//获取车次信息
			if(!HSRAreaConfigDeal.getInstance().checkInit())
				HSRAreaConfigDeal.getInstance().readConfig(conf, hsrIndoorPath, imsipath, trainInfoPath, pointInfoPath);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e);
		}
		return 0;
	}

	public int statData( HSRUserData userData) {
		SceneWorker w = new SceneWorker();
		w.doWork(userData);
		return 0;
	}

	/*public List<RailSecImsi> statData(List<StationImsi> stationImsiList) {
		Worker w = new Worker();
		return w.DoWork(stationImsiList);
	}*/
}
