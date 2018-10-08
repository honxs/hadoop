package cn.mastercom.bigdata.loc.hsr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import cn.mastercom.bigdata.loc.area.HSRAreaInfo;
import cn.mastercom.bigdata.stat.userAna.tableEnums.HsrEnums;
import cn.mastercom.bigdata.util.BitConverter;
import cn.mastercom.bigdata.util.StringHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader.LineHandler;

import org.apache.hadoop.conf.Configuration;

public class HSRConfig
{
	private static HSRConfig instance;

	public static HSRConfig GetInstance()
	{
		if (instance == null)
		{
			instance = new HSRConfig();
		}
		return instance;
	}

	private HSRConfig()
	{
	}

	// TODO 考虑加入方法来返回保护性拷贝 而不是公共访问
	public Map<Long, List<TB_HSR_IMSI_dd_yymmdd>> dicImsiData = null; // IMSI数据<imsi,data>
	public Map<Long, TreeMap<Integer, Double>> dicLocpointData = null; // locpoint数据<trainKey,
																	// <ltime,
																	// startStationDist>>
	public Map<Long, TB_HSR_TRAIN_INFO_dd_yymmdd> dicTrainInfoData = null; // traininfo数据<trainKey,data>
	public Map<Integer, TB_SectionModel> dicSectionData = null; // 区间数据<sectionid,data>
	public Map<Integer, List<TB_SegModel>> dicSegListData = null; // 区间路段数据<sectionid,List<data>>

	public Map<Integer, Set<Long>> sectionEci;//有效区间-小区集合
	public Map<Integer, Set<Long>> stationEci;//有效站点-小区集合
	
	public Map<Long, List<HSRAreaInfo>> imsi_AreaMap;//imsi场景数据<imsi, data>
	public Map<Integer, TB_HSR_STATION_INFO_dd_yymmdd> id_stationMap = null; //站点信息 <stationId, data>
	/**
	 * 加载按天生成的配置
	 * 
	 * @param conf
	 * @param fileDir
	 *            目录路径
	 * @param date
	 *            文件日期,格式:yyMMdd,例如:171122
	 * @return
	 */
	public boolean initDailyCfg(Configuration conf, String fileDir, String date)
	{
		try
		{
			String filePath = HsrEnums.HSR_IMSI.getPath(fileDir, date);
			dicImsiData = new HashMap<>();
			FileReader.readFiles(conf, filePath, new LineHandler() {

				@Override
				public void handle(String line)
				{
					String[] values = line.split("\t", -1);

					TB_HSR_IMSI_dd_yymmdd data = new TB_HSR_IMSI_dd_yymmdd();
					data.trainKey = Long.parseLong(values[0]);
					data.imsi = Long.parseLong(values[1]);
					if(values.length > 2 && StringHelper.isNotBlank(values[2])) 
					{
						data.imei = values[2];
					}
					List<TB_HSR_IMSI_dd_yymmdd> dataList = dicImsiData.get(data.imsi);
					if(dataList == null)
					{
						dataList = new ArrayList<>();
						dicImsiData.put(data.imsi, dataList);
					}
					dataList.add(data);
				}
			});

			filePath = HsrEnums.HSR_LOCATION_POINT.getPath(fileDir, date);
			dicLocpointData = new HashMap<Long, TreeMap<Integer, Double>>();
			FileReader.readFiles(conf, filePath, new LineHandler(){

				@Override
				public void handle(String line)
				{
					String[] values = line.split("\t", -1);

					Long trainKey = Long.parseLong(values[0]);
					Integer itime = Integer.parseInt(values[1]);
					Double startStationDistance = Double.parseDouble(values[2]);
					
					if(startStationDistance <= 0){
						return;
					}
					// 按时间排序
					TreeMap<Integer, Double> sortedLocMap = dicLocpointData.get(trainKey);
					if (sortedLocMap == null)
					{
						sortedLocMap = new TreeMap<Integer, Double>();
						dicLocpointData.put(trainKey, sortedLocMap);
					}
					sortedLocMap.put(itime, startStationDistance);
				}
			});

			filePath = HsrEnums.HSR_TRAIN_INFO.getPath(fileDir, date);
			dicTrainInfoData = new HashMap<Long, TB_HSR_TRAIN_INFO_dd_yymmdd>();
			FileReader.readFiles(conf, filePath, new LineHandler(){

				@Override
				public void handle(String line)
				{
					String[] values = line.split("\t", -1);

					TB_HSR_TRAIN_INFO_dd_yymmdd data = new TB_HSR_TRAIN_INFO_dd_yymmdd();
					data.cityID = Integer.parseInt(values[0]);
					data.trainKey = Long.parseLong(values[1]);
					data.sectionid = Integer.parseInt(values[2]);
					data.trainid = Integer.parseInt(values[3]);
					data.hourTime = Integer.parseInt(values[4]);
					/*data.lstime = Long.parseLong(values[5]);
					data.letime = Long.parseLong(values[6]);*/
					data.istime = Integer.parseInt(values[5]);
					data.ietime = Integer.parseInt(values[6]);
					data.startStationid = Integer.parseInt(values[7]);
					data.endStatiionid = Integer.parseInt(values[8]);
					dicTrainInfoData.put(data.trainKey, data);
				}
			});
			
			/*filePath = HsrEnums.HSR_USER_AREA.getPath(fileDir, date);
			imsi_AreaMap = new HashMap<Long, List<HSRAreaInfo>>();
			FileReader.readFiles(conf, filePath, new LineHandler(){

				@Override
				public void handle(String line) {
					String[] values = line.split("\t");

					HSRAreaInfo data = new HSRAreaInfo();
					data.imsi = Long.parseLong(values[0]);
					data.areaType = Integer.parseInt(values[1]);
					data.areaID = Integer.parseInt(values[2]);
					data.stime = Integer.parseInt(values[3]);
					data.etime = Integer.parseInt(values[4]);
					data.stationID = Integer.parseInt(values[5]);
					if(values.length>6) {
						data.imei = values[6];
					}
					
					List<HSRAreaInfo> dataList = imsi_AreaMap.get(data.imsi);
					if(dataList == null){
						dataList = new ArrayList<>();
						imsi_AreaMap.put(data.imsi, dataList);
					}
					dataList.add(data);
				}
			});*/
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * 加载通用配置
	 * 
	 * @param conf
	 * @param fileDir
	 * @return
	 */
	public boolean initGerneralCfg(Configuration conf, String segFilePath, String sectionFilePath, 
			String secCellFilePath, String stationCellFilePath, String stationFilePath)
	{
		try
		{
			dicSectionData = new HashMap<Integer, TB_SectionModel>();
			FileReader.readFiles(conf, sectionFilePath, new LineHandler() {

				@Override
				public void handle(String line)
				{
					String[] values = line.split("\t", -1);

					TB_SectionModel data = new TB_SectionModel();
//					data.lineid = Integer.parseInt(values[1]);
					data.sectionid = Integer.parseInt(values[1]);
					data.startStationid = Integer.parseInt(values[2]);
					data.endStatiionid = Integer.parseInt(values[3]);
					data.sectionLength = Double.parseDouble(values[4]);
					dicSectionData.put(data.sectionid, data);
				}
			});

			dicSegListData = new HashMap<Integer, List<TB_SegModel>>();
			FileReader.readFiles(conf, segFilePath, new LineHandler(){

				@Override
				public void handle(String line)
				{
					String[] values = line.split("\t", -1);

					TB_SegModel data = new TB_SegModel();
					data.segid = Integer.parseInt(values[1]);
					data.sectionid = Integer.parseInt(values[2]);
					data.distToStart = Double.parseDouble(values[3]);
					data.distToEnd = Double.parseDouble(values[4]);
					data.PointShape = BitConverter.toBytes(values[5]);

					List<TB_SegModel> listChild = dicSegListData.get(data.sectionid);
					if (listChild != null)
					{
						listChild.add(data);
					}
					else
					{
						listChild = new ArrayList<TB_SegModel>();
						listChild.add(data);
						dicSegListData.put(data.sectionid, listChild);
					}
				}
			});
			//按距离排序
			for (List<TB_SegModel> listChild : dicSegListData.values()) 
			{
				Collections.sort(listChild, new Comparator<TB_SegModel>()
				{
					@Override
					public int compare(TB_SegModel o1, TB_SegModel o2)
					{
				
						return (int) (o1.distToStart - o2.distToStart);
					}
				});
			}
			
			sectionEci = new HashMap<>();
			//加载区间小区 + 站点小区
			FileReader.readFiles(conf, secCellFilePath, new LineHandler(){
				@Override
				public void handle(String line)
				{//CityId	区间ID	ECI	小区到起始站距离	小区到终点站距离	
					String[] values = line.split("\t", 4);
					Integer sectionId =  Integer.parseInt(values[1]);
					Set<Long> eciSet = sectionEci.get(sectionId);
					if(eciSet == null)
					{
						eciSet = new HashSet<>();
						sectionEci.put(sectionId, eciSet);
					}
					eciSet.add(Long.parseLong(values[2]));
				}
			});
			stationEci = new HashMap<>();
			FileReader.readFiles(conf, stationCellFilePath, new LineHandler() {
				@Override
				public void handle(String line)
				{//cityId	站点ID	ECI	
					String[] values = line.split("\t", -1);
					Integer stationId =  Integer.parseInt(values[1]);
					Set<Long> eciSet = stationEci.get(stationId);
					if (eciSet == null)
					{
						eciSet = new HashSet<>();
						stationEci.put(stationId, eciSet);
					}
					eciSet.add(Long.parseLong(values[2]));
				}
			});
			
			id_stationMap = new HashMap<Integer, TB_HSR_STATION_INFO_dd_yymmdd>();
			FileReader.readFiles(conf, stationFilePath, new LineHandler() {
						@Override
						public void handle(String line) {
							if(!"".equals(line) && line.length()>0){
								String[] values = line.split("\t", -1);
								int id = Integer.parseInt(values[1]);
								double lng = Double.parseDouble(values[3]);//经度
								double lat = Double.parseDouble(values[4]);
								TB_HSR_STATION_INFO_dd_yymmdd station = 
										new TB_HSR_STATION_INFO_dd_yymmdd(id, lng, lat);
								
								id_stationMap.put(station.id, station);
							}
						}
					});
			
		} catch (Exception e) {
			return false;
		}
		return true;
	}

}
