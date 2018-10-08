package cn.mastercom.bigdata.stat.hsr.old;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import cn.mastercom.bigdata.StructData.GridItemOfSize;
import cn.mastercom.bigdata.StructData.SIGNAL_XDR_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.GisFunction;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

/**
 * Created by Administrator on 2017/6/6.
 */
public class RailFillFunc
{

	public final static double PERLAT_20M = 0.00018f;
	public static double PERLNG_20M = 0.0002f;
	public final static int GRID_SIZE = 20;
	public final static int CELL_HANDOVER_TIME = 30;
	public static float minLng;
	public static float minLat;
	public static HashMap<String, double[]> railtaimap;

	// key 为 eci,double[] 为投影的经纬度

	public static DataAdapterReader dataAdapterReader_MME = null;

	/**
	 * 思路 1. 第一次遍历，将 List<string> 数据变成 ArrayList
	 * <SIGNAL_XDR_4G> cellModelArrayList 2. cellModelArrayList 按照时间排序 3.
	 * 找出cellModelArrayList包含多少个高铁小区(不能重复),找出第一次包含某个高铁小区时，cellModelArrayList对应的
	 * index 下标 这些下标组成一个list ArrayList<Integer> cellIndex 4. 判断是否是高铁用户 5.
	 * 遍历cellIndex, 经纬度回填
	 *
	 * @param
	 * @return
	 */

	public void deal(ArrayList<SIGNAL_XDR_4G> cellModelArrayList)
	{
		// 找出 *第一次* 包含某个高铁小区时，cellModelArrayList对应的 index 下标
		ArrayList<Integer> xdrIndex = getXDRIndex(cellModelArrayList);
		// 判断是否是高铁用户
		boolean ifIsRail = checkRailUser(cellModelArrayList, xdrIndex);
		if (!ifIsRail)
		{
			return;
		}
		LOGHelper.GetLogger().writeLog(LogType.info, "is hiRail user!");
		// 是高铁用户，开始经纬度回填
		fillBackLatLng(cellModelArrayList, xdrIndex);
	}

	private void fillBackLatLng(ArrayList<SIGNAL_XDR_4G> cellModelArrayList, ArrayList<Integer> xdrIndex)
	{
		Iterator<Integer> xdrIndexIterotor = xdrIndex.iterator();
		int lastIndex = 0;
		int thisIndex = 0;
		double thisSpeed = 0.0;
		double lastSpeed = 0.0;
		double distance = 0.0;

		double lastLongitudePerSec = 0.0;
		double lastLatitudePerSec = 0.0;

		while (xdrIndexIterotor.hasNext())
		{
			try
			{
				thisIndex = xdrIndexIterotor.next();
				if (thisIndex == 0)
				{
					continue;
				}
				SIGNAL_XDR_4G lastXdr = cellModelArrayList.get(lastIndex);
				SIGNAL_XDR_4G thisXdr = cellModelArrayList.get(thisIndex);

				// 第一段无法计算，过滤掉
				if (HiRailConfig.cellMap.get(lastXdr.Eci) == null)
				{
					lastIndex = thisIndex;
					continue;
				}
				// 经纬度areaType等的赋值
				if (lastIndex == 0) 
				{
					//还是第一段，无法得到RRU是哪一个，不回填
					lastIndex = thisIndex;
					continue;
				}
				SIGNAL_XDR_4G lastForwardXdr = cellModelArrayList.get(lastIndex - 1);
				SIGNAL_XDR_4G thisForwardXdr = cellModelArrayList.get(thisIndex - 1);
				
				if (!assignLatLng(lastXdr, thisXdr, lastForwardXdr, thisForwardXdr))
				{
					lastIndex = thisIndex;
					continue;
				}
				
				// double speed = getSpeed(lastXdr, thisXdr);
				distance = GisFunction.GetDistance(HiRailConfig.cellMap.get(thisXdr.Eci)[0], HiRailConfig.cellMap.get(thisXdr.Eci)[1], HiRailConfig.cellMap.get(lastXdr.Eci)[0],
						HiRailConfig.cellMap.get(lastXdr.Eci)[1]);
				thisSpeed = distance / (thisXdr.stime - lastXdr.stime);

				if (thisXdr.stime - lastXdr.stime == 0)
				{
					continue;
				}

				// 修改150/3.6 成100/3.6
				if (thisSpeed < 100 / 3.6)
				{
					double[] railLoc = null;
					if (lastSpeed == 0)
					{
						continue;
					}
					
					for (int i = lastIndex; i < thisIndex + 1; i++)
					{
						
						SIGNAL_XDR_4G signal_xdr_4G = cellModelArrayList.get(i);
						// 说明有站台，说明在站台停车了
						if (railtaimap.containsKey(signal_xdr_4G.Eci))
						{
							railLoc = railtaimap.get(signal_xdr_4G.Eci);
							break;
						}
					}
					if(railLoc != null)
					{
						//lastxdr A开始， thisxdr B结束 站台为P, 三点经纬度都已知道	
						if (lastLongitudePerSec == 0)
						{
							continue;
						}
						
						double tAll = thisXdr.stime - lastXdr.stime;
						double sAP = GisFunction.GetDistance(HiRailConfig.cellMap.get(lastXdr.Eci)[0], HiRailConfig.cellMap.get(lastXdr.Eci)[1], railLoc[0], railLoc[1]);
						double tAP = sAP * 2 / lastSpeed;
						double sPB = GisFunction.GetDistance(HiRailConfig.cellMap.get(thisXdr.Eci)[0], HiRailConfig.cellMap.get(thisXdr.Eci)[1], railLoc[0], railLoc[1]);
						double tPB = sPB * 2 / lastSpeed;
						double tPP = tAll - (tAP + tPB);
						// 找AB的角度
						double atanA = Math.atan((thisXdr.latitude - lastXdr.latitude)/((thisXdr.longitude - lastXdr.longitude) * 1.0));
						double latyinzi = Math.cos(Math.abs(atanA));
						double lngyinzi = Math.sin(Math.abs(atanA));
						if (atanA > 0)
						{
							latyinzi = -latyinzi;
						}
						// 计算垂线进行经纬度回填
						double addLng = PERLNG_20M * lngyinzi;
						double addLat = PERLAT_20M * latyinzi;
						
						
						//AP均匀回填， pp停止， pb均匀回填
						for (int i = lastIndex; i < thisIndex + 1; i++){
							SIGNAL_XDR_4G xdrsample = cellModelArrayList.get(i);
							int detTime = xdrsample.stime-lastXdr.stime;
							
							int posLong=0;
							int posLat=0;
							double lineLong=0;
							double lineLat=0;
							double[] railValue = new double[2];
							if(detTime<tAP) 
							{
								lineLong = new Double(detTime * lastLongitudePerSec + lastXdr.longitude).intValue(); // 这里的axAP必须要是负的才行
								lineLat = new Double(detTime * lastLatitudePerSec + lastXdr.latitude).intValue();
									
							} 
							else if(detTime<(tAP+tPP))
							{
								// 停车处理，回填到这个站台
								lineLong =new Double(railLoc[0]).intValue();
								lineLat =new Double(railLoc[1]).intValue();
							}
							else
							{
								lineLong = new Double((detTime - tAP - tPP) * lastLongitudePerSec + railLoc[0]).intValue();
								lineLat = new Double((detTime - tAP - tPP) * lastLongitudePerSec + railLoc[1]).intValue();
//								
							}
							//做垂线
							for (int j = 0; j < 50; j++)
							{
								// 垂线上点的经纬度
								int lngi1 = (int) (lineLong + j * addLng * 10000000.0);
								int lati1 = (int) (lineLat + j * addLat * 10000000.0);
								int lngi2 = (int) (lineLong - j * addLng * 10000000.0);
								int lati2 = (int) (lineLat - j * addLat * 10000000.0);
								
								GridItemOfSize gridItem1 = new GridItemOfSize(-1, lngi1, lati1, GRID_SIZE);
								GridItemOfSize gridItem2 = new GridItemOfSize(-1, lngi2, lati2, GRID_SIZE);
//								
//								GridItem gridItem1 = GridItem.GetGridItem(0, lngi1, lati1);
//								GridItem gridItem2 = GridItem.GetGridItem(0, lngi2, lati2);
								if (HiRailConfig.railGrid.containsKey(gridItem1.tllongitude + "," + gridItem1.tllatitude)){
									railValue = HiRailConfig.railGrid.get(gridItem1.tllongitude + "," + gridItem1.tllatitude);
									posLong = lngi1;
									posLat = lati1;
									LOGHelper.GetLogger().writeLog(LogType.info, "pos station  Left hiRail user!");
									break;
									
								}
								else if (HiRailConfig.railGrid.containsKey(gridItem2.tllongitude + "," + gridItem2.tllatitude)){
									railValue = HiRailConfig.railGrid.get(gridItem2.tllongitude + "," + gridItem2.tllatitude);
									posLong = lngi2;
									posLat = lati2;
									LOGHelper.GetLogger().writeLog(LogType.info, "pos station  Right hiRail user!");
									break;
								}
							}
							if(posLong != 0 && railValue[0] > 0)
							{
								//回填经纬度
								xdrsample.longitude = posLong;
								xdrsample.latitude = posLat;
								xdrsample.longitudeGL = posLong;
								xdrsample.latitudeGL = posLat;
								xdrsample.areaType = (int) railValue[2];
								xdrsample.areaId = (int) railValue[3];
								xdrsample.loctimeGL = xdrsample.stime;
								xdrsample.testType = StaticConfig.TestType_HiRail;
								xdrsample.testTypeGL = StaticConfig.TestType_HiRail;
							}
						}
					}
					lastIndex = thisIndex;				
					continue;
				}
				else if (thisSpeed > (150 / 3.6))
				{
					lastSpeed=thisSpeed;
					// 每秒钟移动的经纬度
					double longitudePerSec = (thisXdr.longitude - lastXdr.longitude) * 1.0 / (thisXdr.stime * 1.0 - lastXdr.stime * 1.0);
					double latitudePerSec = (thisXdr.latitude - lastXdr.latitude) * 1.0 / (thisXdr.stime - lastXdr.stime);
					lastLongitudePerSec = longitudePerSec;
					lastLatitudePerSec = latitudePerSec;
	
					if ((thisXdr.longitude - lastXdr.longitude) == 0)
					{
						continue;
					}
					
					// 计算两点的连线的垂线对应的角度
					double atanA = Math.atan(latitudePerSec / longitudePerSec);
	
					double latyinzi = Math.cos(Math.abs(atanA)); // 本来是cos,
																	// 但是转换一下就是sina
					double lngyinzi = Math.sin(Math.abs(atanA));
					if (atanA > 0)
					{
						latyinzi = -latyinzi;
					}
					// 计算垂线进行经纬度回填
					double addLng = PERLNG_20M * lngyinzi;
					double addLat = PERLAT_20M * latyinzi;
					double[] cellMapValue = HiRailConfig.cellMap.get(thisXdr.Eci);
					for (int i = lastIndex; i < thisIndex; i++)
					{
						SIGNAL_XDR_4G signal_xdr_4G = cellModelArrayList.get(i);
	
						signal_xdr_4G.areaType = (int) cellMapValue[2];
						signal_xdr_4G.areaId = (int) cellMapValue[3];
						signal_xdr_4G.testType = StaticConfig.TestType_HiRail;
						signal_xdr_4G.testTypeGL = StaticConfig.TestType_HiRail;
	
						// 做经纬度回填
						int posLong = (new Double((signal_xdr_4G.stime - lastXdr.stime) * longitudePerSec + lastXdr.longitude)).intValue();
						int posLat = (new Double((signal_xdr_4G.stime - lastXdr.stime) * latitudePerSec + lastXdr.latitude)).intValue();
	

						boolean flag = false;
						for (int j = 0; j < 150; j++)
						{
							// 垂线上点的经纬度
							int lngi1 = (int) (posLong + j * addLng * 10000000.0);
							int lati1 = (int) (posLat + j * addLat * 10000000.0);
							int lngi2 = (int) (posLong - j * addLng * 10000000.0);
							int lati2 = (int) (posLat - j * addLat * 10000000.0);
							
							GridItemOfSize gridItem1 = new GridItemOfSize(-1, lngi1, lati1, GRID_SIZE);
							GridItemOfSize gridItem2 = new GridItemOfSize(-1, lngi2, lati2, GRID_SIZE);
							// 栅格化
//							GridItem gridItem1 = GridItem.GetGridItem(0, lngi1, lati1);
//							GridItem gridItem2 = GridItem.GetGridItem(0, lngi2, lati2);
							//
							if (HiRailConfig.railGrid.containsKey(gridItem1.tllongitude + "," + gridItem1.tllatitude))
							{
								posLong = (int) (posLong + j * addLng * 10000000.0);
								
								posLat = (int) (posLat + j * addLat * 10000000.0);
								double[] railValue = HiRailConfig.railGrid.get(gridItem1.tllongitude + "," + gridItem1.tllatitude);
								signal_xdr_4G.areaType = (int) railValue[2];
								signal_xdr_4G.areaId = (int) railValue[3];
								LOGHelper.GetLogger().writeLog(LogType.info, "pos  Left hiRail user!");
								flag=true;
								break;
							}
							else if (HiRailConfig.railGrid.containsKey(gridItem2.tllongitude + "," + gridItem2.tllatitude))
							{
								posLong = (int) (posLong - j * addLng * 10000000.0);
								posLat = (int) (posLat - j * addLat * 10000000.0);

								double[] railValue = HiRailConfig.railGrid.get(gridItem2.tllongitude + "," + gridItem2.tllatitude);
								signal_xdr_4G.areaType = (int) railValue[2];
								signal_xdr_4G.areaId = (int) railValue[3];
								LOGHelper.GetLogger().writeLog(LogType.info, "pos right hiRail user!");
								flag=true;
								break;
							}
						}
						
						if(flag==true)
						{
							signal_xdr_4G.longitude = posLong;
							signal_xdr_4G.latitude = posLat;
							signal_xdr_4G.longitudeGL = signal_xdr_4G.longitude;
							signal_xdr_4G.latitudeGL = signal_xdr_4G.latitude;
							signal_xdr_4G.loctimeGL = signal_xdr_4G.stime;
							lastIndex = thisIndex;
							LOGHelper.GetLogger().writeLog(LogType.info, "pos  hiRail user!");
						}			
					}
				}	
			}
			catch (Exception e)
			{
				lastIndex = thisIndex;
				e.printStackTrace();
			}
		}
	}

	private boolean assignLatLng(SIGNAL_XDR_4G lastXdr, SIGNAL_XDR_4G thisXdr, SIGNAL_XDR_4G lastForwardXdr, SIGNAL_XDR_4G thisForwardXdr)
	{

		//根据RRU进行赋值操作
		String lastEciToEci = String.valueOf(lastForwardXdr.Eci) + "," + lastXdr.Eci;
		String thisEciToEci = String.valueOf(thisForwardXdr.Eci) + "," + thisXdr.Eci;
		if (HiRailConfig.rruMap.containsKey(lastEciToEci)) 
		{
			//再加个时间判断
			long time = lastXdr.stime - lastForwardXdr.stime;
			if (time > CELL_HANDOVER_TIME)
			{
				//不走下面
				return false;
			}
			lastXdr.longitude = (new Double(HiRailConfig.rruMap.get(lastEciToEci)[0] * 10000000 + "")).intValue();
			lastXdr.latitude = (new Double(HiRailConfig.rruMap.get(lastEciToEci)[1] * 10000000 + "")).intValue();
		}
		else
		{
			lastXdr.longitude = (new Double(HiRailConfig.cellMap.get(lastXdr.Eci)[0] * 10000000 + "")).intValue();
			lastXdr.latitude = (new Double(HiRailConfig.cellMap.get(lastXdr.Eci)[1] * 10000000 + "")).intValue();
		}

		if(HiRailConfig.rruMap.containsKey(thisEciToEci))
		{
			//再加个时间判断
			long time = thisXdr.stime-thisForwardXdr.stime;
			if(time > CELL_HANDOVER_TIME)
			{
				//不走下面
				return false;
			}
			thisXdr.longitude=(new Double(HiRailConfig.rruMap.get(thisEciToEci)[0] * 10000000 + "")).intValue();
			thisXdr.latitude=(new Double(HiRailConfig.rruMap.get(thisEciToEci)[1] * 10000000 + "")).intValue();
		}
		else 
		{
			thisXdr.longitude = (new Double(HiRailConfig.cellMap.get(thisXdr.Eci)[0] * 10000000 + "")).intValue();
			thisXdr.latitude = (new Double(HiRailConfig.cellMap.get(thisXdr.Eci)[1] * 10000000 + "")).intValue();	
		}
		double distinct = GisFunction.GetDistance(thisXdr.longitude/10000000.0, thisXdr.latitude/10000000.0,
				lastXdr.longitude/10000000.0 , lastXdr.latitude/10000000.0);
		if(distinct > 10000)
		{
			return false;
		}
		
		// zks 07-13TODO
		thisXdr.areaType = (int) HiRailConfig.cellMap.get(thisXdr.Eci)[2];
		thisXdr.areaId = (int) HiRailConfig.cellMap.get(thisXdr.Eci)[3];
		thisXdr.testType = StaticConfig.TestType_HiRail;
		thisXdr.testTypeGL = StaticConfig.TestType_HiRail;

		lastXdr.areaType = (int) HiRailConfig.cellMap.get(lastXdr.Eci)[2];
		lastXdr.areaId = (int) HiRailConfig.cellMap.get(lastXdr.Eci)[3];
		lastXdr.testType = StaticConfig.TestType_HiRail;
		lastXdr.testTypeGL = StaticConfig.TestType_HiRail;

		lastXdr.longitudeGL = lastXdr.longitude;
		lastXdr.latitudeGL = lastXdr.latitude;

		thisXdr.longitudeGL = thisXdr.longitude;
		thisXdr.latitudeGL = thisXdr.latitude;
		thisXdr.loctimeGL = thisXdr.etime;
		LOGHelper.GetLogger().writeLog(LogType.info, "fixed hiRail  of two  point !");
		return true;
	}

	private ArrayList<Integer> getXDRIndex(ArrayList<SIGNAL_XDR_4G> cellModelArrayList)
	{
		Random random = new Random();

		ArrayList<Integer> xdrIndex = new ArrayList<Integer>();
		int maxSize = 0;

		for (int i = 0; i < HiRailConfig.cellMapList.size(); i++)
		{
			ArrayList<Integer> xdrIndexX = getXdrIndexByEachMap(cellModelArrayList, HiRailConfig.cellMapList.get(i));
			if (xdrIndexX.size() > maxSize)
			{
				HiRailConfig.cellMap = HiRailConfig.cellMapList.get(i);
				HiRailConfig.railGrid = HiRailConfig.railMapList.get(i);
				xdrIndex = xdrIndexX;
				maxSize = xdrIndexX.size();
			}
			else if (xdrIndexX.size() == maxSize && xdrIndex.size() != 0)
			{
				int nextInt = random.nextInt(2);
				if (nextInt == 1)
				{
					HiRailConfig.cellMap = HiRailConfig.cellMapList.get(i);
					HiRailConfig.railGrid = HiRailConfig.railMapList.get(i);
					xdrIndex = xdrIndexX;
				}
			}
		}

		return xdrIndex;
	}

	public ArrayList<Integer> getXdrIndexByEachMap(ArrayList<SIGNAL_XDR_4G> cellModelArrayList, HashMap<Long, double[]> cellMap)
	{
		ArrayList<Integer> xdrIndex = new ArrayList<>();
		HashMap<Long, Integer> avoidRepeat = new HashMap<>();
		for (int i = 0; i < cellModelArrayList.size(); i++)
		{

			if (cellMap.containsKey(cellModelArrayList.get(i).Eci))
			{
				if (avoidRepeat.containsKey(cellModelArrayList.get(i).Eci))
				{
					continue;
				}
				xdrIndex.add(i);
				avoidRepeat.put(cellModelArrayList.get(i).Eci, 1);
			}
		}
		return xdrIndex;
	}

	private boolean checkRailUser(ArrayList<SIGNAL_XDR_4G> cellModelArrayList, ArrayList<Integer> xdrIndex)
	{
		ArrayList<Integer> newXdrIndex = new ArrayList<>();
		if (xdrIndex.size() < 3)
		{
			return false;
		}

		SIGNAL_XDR_4G lastXDR = cellModelArrayList.get(xdrIndex.get(0));
		int cellCount = 0;
		int begin = 0; //第一次大于150公里每小时，xdrIndex的下标
		int end=0;  // 最后一次大于150公里每小时，xdrIndex的下标
		for (int i = 1; i < xdrIndex.size(); i++)
		{
			SIGNAL_XDR_4G thisXDR = cellModelArrayList.get(xdrIndex.get(i));
			double lastlng = HiRailConfig.cellMap.get(lastXDR.Eci)[0];
			double lastlat = HiRailConfig.cellMap.get(lastXDR.Eci)[1];
			double thislng = HiRailConfig.cellMap.get(thisXDR.Eci)[0];
			double thislat = HiRailConfig.cellMap.get(thisXDR.Eci)[1];
			double dis = GisFunction.GetDistance(thislng, thislat, lastlng, lastlat);
			if ((dis / (thisXDR.stime - lastXDR.stime)) > 150 / 3.6 && (dis / (thisXDR.stime - lastXDR.stime)) < 450 / 3.6)
			{
				if(begin==0){
					begin = i;
				}
				cellCount++;
				end = i;
			}
			lastXDR = thisXDR;
		}
		if (cellCount < 2)
		{
			return false;
		}
		if(end>begin){
			//xdrIndex 进行修改
			for (int j = begin; j < end; j++)
			{
				newXdrIndex.add(xdrIndex.get(j));
			}
			
		}
		xdrIndex.clear();
		xdrIndex.addAll(newXdrIndex);
		return true;
	}
}
