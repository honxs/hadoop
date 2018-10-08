package cn.mastercom.bigdata.stat.userAna.hsr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.stat.userAna.hsr.HSRSecTrainData.LocPoint;
import cn.mastercom.bigdata.stat.userAna.model.HSRUserArea;
import cn.mastercom.bigdata.stat.userAna.model.HSRUserArea.HSRUserAreaType;
import cn.mastercom.bigdata.stat.userAna.model.HSRUserData;
import cn.mastercom.bigdata.stat.userAna.model.HSRUserTrainData;
import cn.mastercom.bigdata.stat.userAna.model.HSRUserXdr;



public class SceneWorker {
	Map<Integer, HSRUserArea> stopIndoorMap = new HashMap<Integer, HSRUserArea>();
	private HSRAreaConfigDeal areaConfigDeal = null;
//	private SceneRecDeal sceneDeal = null;
	private List<HSRUserArea> userAreaList = null;
	private HSRUserArea imsiIndoor = null;
	private HSRUserArea outStation = null;
	private static long minStartTime=-1;//最早开车时间
	private static long maxEndTime=-1;//最晚停车时间
	private static int firstStation = -1;
	private static int finalStation = -1;
	private HSRUserArea stopBean = null;
	private HSRUserTrainData trainData = null;
	private static String imsi="";
	
	//高铁用户场景分析
		public void doWork(HSRUserData userData) {
			
			userAreaList = userData.userAreaList;//用户所在的区域
			areaConfigDeal = HSRAreaConfigDeal.getInstance();
			Map<Long, Integer> eci_indoor_map = areaConfigDeal.eci_indoor_map;		
			List<HSRUserTrainData> imsiTrainInfo = null;
			 		
			imsi = userData.userXdrList.get(0).imsi;
			imsiTrainInfo = getImsiTrainInfo(imsi,imsiTrainInfo);//获取imsi的车次信息
			if(imsiTrainInfo == null) return;
														
			for (HSRUserXdr userXdr : userData.userXdrList){
				
				long ecitime = userXdr.time;
				long eci = userXdr.eci;	
				int stationId = -1;
				if(eci_indoor_map.containsKey(eci))
					stationId = eci_indoor_map.get(eci);						
				if ( stationId > 0 ){
					//出发(候车)/出站/经停
					if (stationId == firstStation){
						imsiIndoor = imsiAreaAna(imsi, ecitime, imsiIndoor, HSRUserAreaType.WAIT_ROOM, stationId);					
					}else if (stationId == finalStation){
						outStation = imsiAreaAna(imsi, ecitime, outStation, HSRUserAreaType.OUT_STATION, stationId);
					}else{
						getStopIndoopData(ecitime, stationId);											
					}																	
				}	
			}
			
			//进站/第一个有记录的站
			firstStationDeal(imsiTrainInfo);
			//出站/有记录的最后一站
			fanalStationDeal(imsiTrainInfo);
			//停经
			throughStationDeal(imsiTrainInfo);																	
		}
		
		/**
		 * 经停
		 * 
		 * @param imsiTrainInfo
		 */
		public void throughStationDeal(List<HSRUserTrainData> imsiTrainInfo){
			for(int i = 0; i < imsiTrainInfo.size()-1; i++){
				trainData = imsiTrainInfo.get(i);
				int endStationId = trainData.endStatiionid;
				long lstime = trainData.lstime;
				long letime = trainData.letime;//停车时间(如果没有室分小区数据就车次的停车时间)
				long nextLstime = 0;//下趟车开车时间
				if(!stopIndoorMap.isEmpty()){
					if(stopIndoorMap.containsKey(endStationId)){
						letime = stopIndoorMap.get(endStationId).beginTime;
						//如果访问过室分小区就按访问室分小区的最晚时间当成驶离的最初时间
						nextLstime = stopIndoorMap.get(endStationId).endTime;
					}
				}
				//驶入 	
				long intoTime = intoORLeavePalt(trainData, 2, lstime);
				//intoTime = lstime + intoTime;
				if(intoTime > letime || intoTime<=lstime){
					intoTime = letime  - 60*1000;
					//intoTime = (letime +endTime)/2;
				} 
				addImsiArea(imsi,intoTime, letime, HSRUserAreaType.INTO_PLAT, endStationId);
				//停经(停车-下趟车开车时间)
				trainData = imsiTrainInfo.get(i+1);
				int nextStationId = trainData.startStationid;//其实也是上趟车的endStationId
				if(nextLstime == 0){
					nextLstime = trainData.lstime;
				}
//				long nextLetime = trainData.letime;
				
				if (nextLstime <= letime ) nextLstime = (long)(letime + 120*1000);
				addImsiArea(imsi,letime, nextLstime, HSRUserAreaType.STOP_PLAT, nextStationId); 
				//驶离
				long leaveTime = intoORLeavePalt(trainData, 1, nextLstime);	
				//leaveTime = nextLstime + leaveTime;
				if (leaveTime <= nextLstime ) {
					leaveTime = nextLstime + 60*1000;
					// leaveTime = (nextLstime +beginTime)/2;
				}
				addImsiArea(imsi,nextLstime, leaveTime, HSRUserAreaType.LEAVE_PLAT, nextStationId);
			}	
		}
		
		//出站
		public void fanalStationDeal(List<HSRUserTrainData> imsiTrainInfo){
			if(outStation != null){
				//到达			
				long stime=outStation.beginTime;//室分小区开始记录时间
//				long etime=outStation.endTime;
				trainData = imsiTrainInfo.get(imsiTrainInfo.size()-1);
				long finalSTime = imsiTrainInfo.get(imsiTrainInfo.size()-1).lstime;//最后一个车次的开车时间
				//停车时间用进入室分小区前3分钟的时间
				maxEndTime = stime - 180*1000;			

				long intoTime = intoORLeavePalt(trainData, 2, finalSTime);//驶入到始发站的时间			
				if(intoTime <= finalSTime ||intoTime > maxEndTime){
					intoTime = maxEndTime - 60*1000;
				}
				addImsiArea(imsi,intoTime, maxEndTime, HSRUserAreaType.INTO_PLAT, finalStation);//驶入
				//addImsiArea(imsi,maxEndTime, etime, HSRUserAreaType.OUT_STATION); 
			}else{
				//上海的最后一站没出站(经停)
				trainData = imsiTrainInfo.get(imsiTrainInfo.size()-1);
				long finalSTime = imsiTrainInfo.get(imsiTrainInfo.size()-1).lstime;//最后一个车次的开车时间
				long finalETime = imsiTrainInfo.get(imsiTrainInfo.size()-1).letime;
				long intoTime = intoORLeavePalt(trainData, 2, finalSTime);
				if(intoTime <= finalSTime ||intoTime > finalETime){
					intoTime = finalETime - 60*1000;
				}
				addImsiArea(imsi,intoTime, finalETime, HSRUserAreaType.INTO_PLAT, finalStation);//驶入
			}
		}
		
		//进站
		public void firstStationDeal(List<HSRUserTrainData> imsiTrainInfo){
			
			if(imsiIndoor != null){
				//出发			
				long indoorEtime=imsiIndoor.endTime;
				trainData = imsiTrainInfo.get(0);
				minStartTime = indoorEtime + 300*1000;
				long leaveTime = intoORLeavePalt(trainData, 1, minStartTime);//距离始发站两公里的地方的时间
//				if(imsi.equals("459996391968817_3577560705559804")){
//					leaveTime = intoORLeavePalt(trainData, 1);
//				}
//				addImsiArea(imsi, indoorEtime, minStartTime, HSRUserAreaType.PLATFORM, firstStation);//站台
				addImsiArea(imsi, minStartTime, leaveTime, HSRUserAreaType.LEAVE_PLAT, firstStation);//驶离
			}else{
				//第一个经停站
				trainData = imsiTrainInfo.get(0);
				if(trainData == null) 
					return;
				long firstSTime = trainData.lstime;//开车时间
				long firstETime = trainData.letime;//停车时间
//				firstStation = trainData.startStationid;
				long leaveTime = intoORLeavePalt(trainData, 1, firstSTime);//距离始发站两公里的地方的时间			
				if (leaveTime <= firstSTime || leaveTime >=firstETime ) {
					leaveTime = firstSTime + 60*1000;
				}
				addImsiArea(imsi, firstSTime, leaveTime, HSRUserAreaType.LEAVE_PLAT, firstStation);//驶离						
			}
		}
		//获取所有经停时访问室分小区的最早最晚时间<stationId,HSRUserArea>
		public void getStopIndoopData(long ecitime, int stationId){
			
			if(!stopIndoorMap.isEmpty() && stopIndoorMap.containsKey(stationId)){
				stopBean = stopIndoorMap.get(stationId);
				stopBean.addEciTime(ecitime);
			}else{
				stopBean = new HSRUserArea(ecitime);
				stopIndoorMap.put(stationId, stopBean);
			}
		}
		//计算距离站点2公里的时间
		@SuppressWarnings("unchecked")
		public long intoORLeavePalt(HSRUserTrainData trainData, int type, long startTime){
			int count = 1;
			double length = trainData.distance;
			long[] times = new long[3];
			double[] distances = new double[3];
			double distance = 0;
			List<LocPoint> pointList = trainData.pointList;
			if(pointList==null || pointList.isEmpty()){
				System.out.println("没有定位点");
				return 0;
			}
			//用靠近2公里的两个点的平均速度得出2公里处的时间，如果只有一个点就当此处和2公里处的速度是一样的
			if(type==1){//type=1时为驶离
				for(int i=0; i < pointList.size();i++){		
					LocPoint locPoint = pointList.get(i);
					distances[0] = locPoint.startStationDistance;			
					if(distances[0] < 2500){//算距离始发站2公里的时间
						distances[count] = distances[0];
						times[count] = locPoint.ltime;	
						count++;
					}	
					if(count>=3) break;				
				}
				if(pointList.size()>1 && count<3){															
					distances[1] =  pointList.get(0).startStationDistance;
					times[1] = pointList.get(0).ltime;	
					distances[2] =  pointList.get(1).startStationDistance;
					times[2] = pointList.get(1).ltime;					
				}			
				distance = 2000;
			}else{
				for(int i = pointList.size()-1; i > 0;i--){		
					LocPoint locPoint = pointList.get(i);
					distances[0] = length - locPoint.startStationDistance;			
					if(1500 < distances[0] && distances[0] < 2500){//算距离始发站2公里的时间
						distances[count] = distances[0];//count 初始为1
						times[count] = locPoint.ltime;	
						count++;
					}	
					if(count>=3) break;				
				}
				distance = length - 2000;
			}
			long disTime =0;
			if(pointList.size()>1 && count<3){															
				distances[1] =  pointList.get(pointList.size()-1).startStationDistance;
				times[1] = pointList.get(pointList.size()-1).ltime;	
				distances[2] =  pointList.get(pointList.size()-2).startStationDistance;
				times[2] = pointList.get(pointList.size()-2).ltime;					
			}	
			distances[0] = Math.abs(distances[2] - distances[1]);
			times[0] = Math.abs(times[2] - times[1]);
			disTime = (long)(Math.abs(distance - distances[1])*times[0]/distances[0]);			
			return disTime + times[1];
		}
		

		//得到imsi所经历过得车次
		public List<HSRUserTrainData> getImsiTrainInfo(String imsi, List<HSRUserTrainData> imsiTrainInfo){
			imsiTrainInfo = new ArrayList<HSRUserTrainData>();
			List<Long> trainList = areaConfigDeal.imsi_secMap.get(imsi);
			List<HSRSecTrainData> train_InfoList = areaConfigDeal.train_InfoList;
			Map<Long, List<LocPoint>> sec_PoinMap = areaConfigDeal.sec_PoinMap;
			if(trainList==null ||trainList.isEmpty()){
				return null;
			}
			for(long trainKey : trainList){
				List<LocPoint> pointList = sec_PoinMap.get(trainKey);
				if(pointList == null || pointList.isEmpty()) continue;
				sortPointList(pointList);//按定位点的时间进行排序
				HSRUserTrainData userTrainData = new HSRUserTrainData(trainKey, pointList);
				for(HSRSecTrainData trainData : train_InfoList){
					if(trainKey == trainData.trainkey){
						userTrainData.distance = trainData.distance;
						userTrainData.startStationid = trainData.startStationid;
						userTrainData.endStatiionid = trainData.endStatiionid;
						userTrainData.lstime = trainData.lstime;
						userTrainData.letime = trainData.letime;										
					}
				}
				
				imsiTrainInfo.add(userTrainData);
			}
			
			sortlsTime(imsiTrainInfo);//按开车时间进行排序
			if(imsiTrainInfo.size()<1) 
				return null;				
			firstStation = imsiTrainInfo.get(0).startStationid;
			finalStation = imsiTrainInfo.get(imsiTrainInfo.size()-1).endStatiionid;	
			return imsiTrainInfo;
		}
				
		//入站进站
//		public HSRUserArea intoStationAna(String imsi, Map<Long, Integer> eci_indoor_map, int flag){
//			long minsTime = SceneRecDeal.minsTime;
//			long maxeTime = SceneRecDeal.maxeTime;
//			if (flag==1) {
//				HSRUserArea indoorRoom = new HSRUserArea(imsi, minsTime, HSRUserAreaType.WAIT_ROOM, flag);
//				userAreaList.add(indoorRoom);
//				return indoorRoom;
//			} else if (flag==2){
//				HSRUserArea outStation = new HSRUserArea(imsi, maxeTime, HSRUserAreaType.OUT_STATION, flag);
//				userAreaList.add(outStation);
//				return outStation;
//			}
//			return null;
//		} 
		//insert各场景情况
		public HSRUserArea imsiAreaAna(String imsi, long time, HSRUserArea AreaEntity, HSRUserAreaType type, int stationId){
					
			if (AreaEntity==null && !userAreaList.contains(AreaEntity)){
				AreaEntity = new HSRUserArea(imsi, time, stationId);
				AreaEntity.areaType= type;//场景类型
				userAreaList.add(AreaEntity);
			}else{
				AreaEntity.addEciTime(time);
			}
			
			return AreaEntity;
		} 
		public void addImsiArea(String imsi, long stime, long etime, HSRUserAreaType type, int stationId){
			HSRUserArea areaEntity = new HSRUserArea(imsi, stime, etime, type, stationId);
			userAreaList.add(areaEntity);
		}
		//一个imsi的所有车次按开车时间排序
		public void sortlsTime(List<HSRUserTrainData> list){  
		    Collections.sort(list, new Comparator<HSRUserTrainData>(){
		    	@Override  
		        public int compare(HSRUserTrainData train1, HSRUserTrainData train2) {  
		    		if(train1.getLstime() > train2.getLstime()){ 
		                return 1;
		            }else if(train1.getLstime() == train2.getLstime()){  
		                return 0;
		            }else{
		                return -1;
		            }
		        }
		    });
		}
		
		public void sortPointList(List<LocPoint> list){  
		    Collections.sort(list, new Comparator<LocPoint>(){
		    	@Override  
		        public int compare(LocPoint point1, LocPoint point2) {  
		    		
		            if(point1.getLtime() > point2.getLtime()){ 
		                return 1;
		            }else if(point1.getLtime() == point2.getLtime()){  
		                return 0;
		            }else{
		                return -1;
		            }
		        }
		    });
		}
		
		//经停访问室分小区
		public class StopIndoor {
			
	        public int stationId;
	        public long time;
	        public long stime;
	        public long etime;
	        public long eci;
	        
	        public StopIndoor(){
	        	
	        }
	    }
}
