package cn.mastercom.bigdata.stat.userAna.hsr;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.mroxdrmerge.AppConfig;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userAna.hsr.HSRSecTrainData.LocPoint;
import cn.mastercom.bigdata.stat.userAna.hsr.HSRSecTrainData.TimeSpan;
import cn.mastercom.bigdata.stat.userAna.hsr.HSRSecTrainData.TrainSeg;
import cn.mastercom.bigdata.stat.userAna.model.ImsiInfoSimple;
import cn.mastercom.bigdata.stat.userAna.model.Xdr_ImsiEciTime;
import cn.mastercom.bigdata.stat.userAna.tableEnums.HsrEnums;
import cn.mastercom.bigdata.util.IDataDeal;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.data.Tuple2;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader.LineHandler;

/**
 * @author Kwong
 * 计算并吐出中间表
 */
public class HSRUserAnaDeal implements IDataDeal{

	private static final String OUT_DATA_SEPARATOR = "\t";
	
	ResultOutputer resultOutputer; 
	Configuration conf;
	
	int sectionId;
	boolean oneToTwo;
	int hourTime;
	
	HSRUserAna hsrUserAna = new HSRUserAna();
	HSRData hsrData = new HSRData();
	
	public HSRUserAnaDeal(ResultOutputer resultOutputer){
		this.resultOutputer = resultOutputer;
	
		conf = MainModel.GetInstance().getConf();
		AppConfig appConfig = MainModel.GetInstance().getAppConfig();
		hsrUserAna.init(conf, appConfig.getHsrStationPath(), appConfig.getHsrSectionPath(), appConfig.getHsrStationCellPath(), appConfig.getHsrSectionCellPath(), appConfig.getHsrSectionRruPath(), appConfig.getHsrSegmentPath(), appConfig.getHsrIndoorPath() );
	}
	
	public void init(Tuple2<Integer, Boolean> t){
		this.sectionId = t.first;
		this.oneToTwo = t.second;
	}
	
	public void init(int sectionId, boolean oneToTwo, int hourTime){
		this.sectionId = sectionId;
		this.oneToTwo = oneToTwo;
		this.hourTime = hourTime;
	}
	
	@Override
	public int pushData(int code, String value) {
		String[] strs = value.split("\t");
		
		Xdr_ImsiEciTime xdrLable = new Xdr_ImsiEciTime();
		xdrLable.imsi = strs[0];
		xdrLable.time = Long.parseLong(strs[1]);
		xdrLable.eci = Long.parseLong(strs[2]);
		//Step 1: 添加相关 的XDR_LOCATION数据
		hsrData.xdrLocationList.add(xdrLable);
		return 0;
	}

	@Override
	public void statData() {
		
		if(hsrData.xdrLocationList == null || hsrData.xdrLocationList.isEmpty()){
			return;
		}
		
		//Step 2: 加载区间用户数据
		String baseOutputPath = conf.get("mapreduce.job.output.path");
		String date = conf.get("mapreduce.job.date");
		String filePath = HsrEnums.HSR_TMP_POTENTIAL_USER.getPath(baseOutputPath, date);
		RailSecImsi railSegImsi = loadImsiInfoBySec(filePath, sectionId, oneToTwo);	
		
		if(railSegImsi == null || (railSegImsi.imsiInfoSimpleMap1.isEmpty() && railSegImsi.imsiInfoSimpleMap2.isEmpty())) 
			return;
		//Step 3： 统计
		hsrUserAna.statData(railSegImsi, hsrData);
	}

	@Override
	public void outData() {
		for(HSRSecTrainData hsrSecTrainData : hsrData.secTrainDataList){
			
			//180119 add 如果车次开车时间 不在当前的三小时分区内，去掉
			// hsrSecTrainData.hourTime： 17011910    hourTime: 10
			if(hourToDeal(hsrSecTrainData.hourTime % 100)!= hourToDeal(hourTime)){
				continue;
			}
			try
			{
				StringBuilder sb = new StringBuilder();
				//Step 4.1: 吐出 <车次信息> 
				sb.append(hsrSecTrainData.cityID).append(OUT_DATA_SEPARATOR)
				.append(hsrSecTrainData.trainkey).append(OUT_DATA_SEPARATOR)
				.append(hsrSecTrainData.sectionid).append(OUT_DATA_SEPARATOR)
				.append(hsrSecTrainData.trainid).append(OUT_DATA_SEPARATOR)
				.append(hsrSecTrainData.hourTime).append(OUT_DATA_SEPARATOR)
				.append(hsrSecTrainData.lstime/1000).append(OUT_DATA_SEPARATOR)//5
				.append(hsrSecTrainData.letime/1000).append(OUT_DATA_SEPARATOR)
				.append(hsrSecTrainData.startStationid).append(OUT_DATA_SEPARATOR)
				.append(hsrSecTrainData.endStatiionid).append(OUT_DATA_SEPARATOR)
				.append(hsrSecTrainData.imsiList.size()).append(OUT_DATA_SEPARATOR)
				.append(hsrSecTrainData.stopStart).append(OUT_DATA_SEPARATOR)
				.append(hsrSecTrainData.stopEnd);/*.append(OUT_DATA_SEPARATOR)
				.append(hsrSecTrainData.distance)*/;//12
				if(sb.length() > 0)
					resultOutputer.pushData(HsrEnums.HSR_TRAIN_INFO.getIndex(), sb.toString());
				
				sb.delete(0, sb.length());
				//Step 4.2: 吐出 <车次定位点>
				for(LocPoint locPoint : hsrSecTrainData.locPointList){
					sb.append(hsrSecTrainData.trainkey).append(OUT_DATA_SEPARATOR)
					.append(locPoint.ltime/1000).append(OUT_DATA_SEPARATOR)
					.append(locPoint.startStationDistance).append(OUT_DATA_SEPARATOR)
					.append(locPoint.EciList)
					.append("\n");
				}
				if(sb.length() > 0)
					resultOutputer.pushData(HsrEnums.HSR_LOCATION_POINT.getIndex(), sb.substring(0, sb.length()-"\n".length()));
				
				sb.delete(0, sb.length());
				//Step 4.3：吐出 <车次用户Imsi>
				for(ImsiInfoSimple imsiInfoSimpe : hsrSecTrainData.imsiList){
					String[] imsiImei =  imsiInfoSimpe.imsi.split("_");
					sb.append(hsrSecTrainData.trainkey).append(OUT_DATA_SEPARATOR)
					.append(imsiImei[0]).append(OUT_DATA_SEPARATOR)
					.append(imsiImei.length > 1 ? imsiImei[1] : "").append("\n");
				}
				if(sb.length() > 0)
					resultOutputer.pushData(HsrEnums.HSR_IMSI.getIndex(), sb.substring(0, sb.length()-"\n".length()));
				
				sb.delete(0, sb.length());
				//Step 4.4: 吐出 <车次无覆盖时间段>
				for(TimeSpan timespan : hsrSecTrainData.noCoverTimeSpanList){
					sb.append(hsrSecTrainData.trainkey).append(OUT_DATA_SEPARATOR)
					.append(timespan.lstime/1000).append(OUT_DATA_SEPARATOR)
					.append(timespan.letime/1000).append("\n");
				}
				if(sb.length() > 0)
					resultOutputer.pushData(HsrEnums.HSR_NOVER_TIME.getIndex(), sb.substring(0, sb.length()-"\n".length()));
				
				sb.delete(0, sb.length());
				//Step 5：吐出<车次路段信息>
				for(TrainSeg trainSeg : hsrSecTrainData.segList){
					sb.append(hsrSecTrainData.cityID).append(OUT_DATA_SEPARATOR)
					.append(hsrSecTrainData.trainkey).append(OUT_DATA_SEPARATOR)
					.append(hsrSecTrainData.sectionid).append(OUT_DATA_SEPARATOR)
					.append(trainSeg.segId).append(OUT_DATA_SEPARATOR)
					.append((int)trainSeg.stime).append(OUT_DATA_SEPARATOR)
					.append((int)(trainSeg.stime + trainSeg._t)).append(OUT_DATA_SEPARATOR)
					.append((float)trainSeg.avgSpeed)
					.append("\n");
				}
				if(sb.length() > 0)
					resultOutputer.pushData(HsrEnums.HSR_SEG_INFO.getIndex(), sb.substring(0, sb.length()-"\n".length()));
				
				sb.delete(0, sb.length());
				
				//Step 6：吐出<车次无覆盖路段>
				for(Integer segId : hsrSecTrainData.noCoverSegIdList){
					sb.append(hsrSecTrainData.cityID).append(OUT_DATA_SEPARATOR)
					.append(hsrSecTrainData.trainkey).append(OUT_DATA_SEPARATOR)
					.append(hsrSecTrainData.sectionid).append(OUT_DATA_SEPARATOR)
					.append(segId).append("\n");
				}
				if(sb.length() > 0)
					resultOutputer.pushData(HsrEnums.HSR_SEG_NOCOVER.getIndex(), sb.substring(0, sb.length()-"\n".length()));
				
				sb.delete(0, sb.length());
				
				//Step 7：吐出<高铁用户的xdr>
				for(ImsiInfoSimple imsiInfoSimpe : hsrSecTrainData.imsiList){
					//double speed 
					List<Xdr_ImsiEciTime> xdrList = imsiInfoSimpe.xdrRecordList;
					for(Xdr_ImsiEciTime xdrRecord : xdrList){
						sb.append(xdrRecord.imsi).append(OUT_DATA_SEPARATOR)//0
						.append(xdrRecord.eci).append(OUT_DATA_SEPARATOR)//1
						.append(xdrRecord.time).append(OUT_DATA_SEPARATOR)//2
//						.append(minBeginTime).append(OUT_DATA_SEPARATOR)//3
//						.append(beginTime).append(OUT_DATA_SEPARATOR)//4
//						.append(endTime).append(OUT_DATA_SEPARATOR)//5
//						.append(maxEndTime).append(OUT_DATA_SEPARATOR)//6
						.append("\n");
					}
					
				}
				if(sb.length() > 0)
					resultOutputer.pushData(HsrEnums.HSR_XDR_INFO.getIndex(), sb.substring(0, sb.length()-"\n".length()));
				sb.delete(0, sb.length());
				/*Step 8：吐出<用户场景>
				for(Map.Entry<String, List<HSRUserArea>> entry : hsrSecTrainData.userAreaMap.entrySet()){
					String[] imsi_imei = entry.getKey().split("_");
					List<HSRUserArea> userArealist = entry.getValue();					
					for(HSRUserArea userArea : userArealist){
						sb.append(hsrSecTrainData.cityID).append(OUT_DATA_SEPARATOR)
						.append(hsrSecTrainData.trainkey).append(OUT_DATA_SEPARATOR)
						.append(imsi_imei[0]).append(OUT_DATA_SEPARATOR)
						.append(userArea.areaType).append(OUT_DATA_SEPARATOR)
						.append(userArea.beginTime).append(OUT_DATA_SEPARATOR)
						.append(userArea.endTime)
						.append("\n");
					}					
				}
				if(sb.length() > 0)
					resultOutputer.pushData(HsrEnums.HSR_USER_AREA.getIndex(), sb.substring(0, sb.length()-"\n".length()));
				
				sb.delete(0, sb.length());*/
			
			}
			catch (Exception e)
			{
				e.printStackTrace();

				LOGHelper.GetLogger().writeLog(LogType.error,"HSR DATA OUTPUT ERROR", "HSR DATA OUTPUT ERROR ", e);
			}
		}
		//吐出用户场景分析结果
		
		
		hsrData = new HSRData();
		this.sectionId = -1;
		this.oneToTwo = false;
	}
	
	private RailSecImsi loadImsiInfoBySec(String path, final int sectionId, final boolean oneToTwo){
		final Map<String, ImsiInfoSimple> imsiInfoSimpleMap1 = new HashMap<>();
		final Map<String, ImsiInfoSimple> imsiInfoSimpleMap2 = new HashMap<>();
		try
		{
			FileReader.readFiles(conf, path, new LineHandler(){
				//格式：sectionId	1/2	imsi	beginTime	endTime	endTimeFixed	speed
				@Override
				public void handle(String line)
				{
					String[] strArr = line.split("\t");
					int sectionID = Integer.parseInt(strArr[0]);
					if(sectionId != sectionID){
						return;
					}
					int iOneToTwo = Integer.parseInt(strArr[1]);
					String imsi = strArr[2];
					long beginTime = Long.parseLong(strArr[3]);
					long endTime = Long.parseLong(strArr[4]);
					long endTimeFixed = Long.parseLong(strArr[5]);
					double speed = Double.parseDouble(strArr[6]);
					String imsiOrg = strArr[7];
					long minBeginTime = Long.parseLong(strArr[8]);
					long maxEndTime = Long.parseLong(strArr[9]);
					
					ImsiInfoSimple imImsiInfoSimple = new ImsiInfoSimple(imsi, beginTime, endTime, endTimeFixed, speed, imsiOrg, minBeginTime, maxEndTime);
					
					if(iOneToTwo == RailSecImsi.ONE_TO_TWO && oneToTwo){
						imsiInfoSimpleMap1.put(imsi, imImsiInfoSimple);
					}else if(iOneToTwo == RailSecImsi.TWO_TO_ONE && !oneToTwo){
						imsiInfoSimpleMap2.put(imsi, imImsiInfoSimple);
					}
				}
			});
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new RuntimeException("初始化高铁潜在用户异常");
		}
		RailSecImsi railSegImsi = new RailSecImsi(sectionId, imsiInfoSimpleMap1, imsiInfoSimpleMap2);
		return railSegImsi;
	}

	/**
	 * 对任意一小时 都返回一个值，该值用于分区，返回相同值的时间段将一起处理
	 * @param hourTime
	 * @return 处理连续3个小时内的最后一个小时
	 */
	public static int hourToDeal(int hourTime){
		hourTime = Math.abs(hourTime % 24);
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.GanSu)){//甘肃需要8、9、10作为统计时间段
			return ((hourTime + 1) / 3 * 3 + 1) % 24;
		}
		return (hourTime / 3 * 3 + 2) % 24;
	}

	/**
	 * 对任意一小时 都返回一个值，该值用于分区，返回相同值的时间段将一起处理
	 * @param date
	 * @return 处理连续3个小时内的最后一个小时
	 */
	public static Date dateToDeal(Date date){
		Date dateToDeal = new Date(date.getTime());
		int hour = date.getHours();
		int hourToDeal = hourToDeal(hour);
		if (hourToDeal < hour){
			TimeUtil.addDays(dateToDeal,1);
		}
		dateToDeal.setHours(hourToDeal);

		return dateToDeal;
	}
}
