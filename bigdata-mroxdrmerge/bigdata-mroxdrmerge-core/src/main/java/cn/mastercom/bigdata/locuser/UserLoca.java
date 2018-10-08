package cn.mastercom.bigdata.locuser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

import org.apache.commons.math3.util.Pair;

public class UserLoca
{
	public Map<Integer, DoorList> doors = new HashMap<Integer, DoorList>();
	public List<LocaData> samlocs = new ArrayList<LocaData>();
	public CfgInfo cInfo = null;
	private double[] r_offset = new double[114];
	CfgSimu cs = new CfgSimu();

	public UserLoca()
	{
		InitOffset();
	}

	public void Clear()
	{
		samlocs.clear();
		doors.clear();
		cInfo = null;
	}

	public void GetLoca(List<MrSplice> samsprop, CfgInfo cIf, Map<String, Integer> figss)
	{
		Clear();

		cInfo = cIf;

		LOGHelper.GetLogger().writeLog(LogType.info, "init loc..." + samsprop.size());
		for (int ii = 0; ii < samsprop.size(); ii++)
		{
			int eci = samsprop.get(ii).outeci;

			if (!doors.containsKey(eci))
			{
				doors.put(eci, new DoorList());
			}

			DoorList sam = doors.get(eci);

			if ((samsprop.get(ii).doortype == 1) || (samsprop.get(ii).doortype == 3))
			{
				sam.indoors.add(samsprop.get(ii));
			}
			else if ((samsprop.get(ii).doortype == 2) || (samsprop.get(ii).doortype == 4))
			{
				sam.otdoors.add(samsprop.get(ii));
			}
		}

		GetLoca(figss);
	}

	private void GetLoca(Map<String, Integer> figss)
	{
		LOGHelper.GetLogger().writeLog(LogType.info, "loc samples...");

		

		// 用于记录上一个已处理的切片信息，用于后续定位时的参考判断
		LocSpliceMidInfo lcSpliceMidInfo = new LocSpliceMidInfo();

		// 同一个服务小区的采样点
		for (Map.Entry<Integer, DoorList> kp : doors.entrySet())
		{
			// 获得一个小区的栅格数据
			cs.GetSimu(kp.getKey(), cInfo, figss);

			// if ((cs.ingrids.size() >= 0) && (kp.getValue().indoors.size() > 0))
			if (kp.getValue().indoors.size() > 0)
			{
				LOGHelper.GetLogger().writeLog(LogType.info, "ingrids = " + String.valueOf(cs.ingrids.size()) 
					+ " sam = " + String.valueOf(kp.getValue().indoors.size()));

				for (int ii = 0; ii < kp.getValue().indoors.size(); ii++)
				{
					// 分析一个采样点
					if (cs.ingrids.size() > 0)
					{
						LOGHelper.GetLogger().writeLog(LogType.info, "AnaSam = " + String.valueOf(ii)); 						
						
						AnaSam(cs.ingrids, kp.getValue().indoors.get(ii), lcSpliceMidInfo, 1);
					}
					else
					{
						//LOGHelper.GetLogger().writeLog(LogType.info, "ana sam with no ingrids.");
						// 要给未定位上的赋小区经纬度，所以这里注释掉
						// kp.getValue().indoors.get(ii).doortype = 2;
						// 170318 不用室外定位了
						// AnaSam(cs.otgrids, kp.getValue().indoors.get(ii), lcSpliceMidInfo, 0);
						AnaSam(kp.getValue().indoors.get(ii));
					}
					
					//LOGHelper.GetLogger().writeLog(LogType.info, "end ana indoor smaple.");
				}
			}

			if ((cs.otgrids.size() >= 0) && (kp.getValue().otdoors.size() > 0))
			{
				LOGHelper.GetLogger().writeLog(LogType.info, "otgrids = " + String.valueOf(cs.otgrids.size())
						+ " sam = " + String.valueOf(kp.getValue().otdoors.size()));
				for (int ii = 0; ii < kp.getValue().otdoors.size(); ii++)
				{
					// 分析一个采样点
					AnaSam(cs.otgrids, kp.getValue().otdoors.get(ii), lcSpliceMidInfo, 0);
				}
			}
		}

		//cs.Clear();
		doors.clear();
	}
	
    private void AnaSam(MrSplice splice)
    {
    	// 20170311 cai的需求一个小区对多个楼宇
        //int bid = GetBuildId(splice);
    	String bid = GetBuildId(splice);
        LocaData lcda = new LocaData();
        // lcda.buildingid = bid;        
        int aa = bid.indexOf('#', 1);
        if (aa > 1)
        {
        	lcda.buildingid = Integer.parseInt(bid.substring(1, aa));
        }
        
        lcda.longitude = (int)(splice.scell.cell.longitude * 10000000);
        lcda.latitude = (int)(splice.scell.cell.latitude * 10000000);
        lcda.ilevel = 0;
        lcda.s1apid = splice.MmeUeS1apId;
        lcda.eci = splice.eci;
        lcda.rsrp = (splice.scell.cell.rsrp_avg / splice.scell.cell.rsrp_cnt);
        lcda.property = splice.property;
        lcda.doortype = splice.doortype;
        lcda.section_btime = splice.section_btime;// 段落开始时间
        lcda.section_etime = splice.section_etime;// 段落结束时间
        lcda.splice_btime = splice.splice_btime; // 切片开始时间
        lcda.splice_etime = splice.splice_etime; // 切片结束时间
        lcda.isamplecnt = splice.scell.cell.rsrp_cnt;
        lcda.isamebtscnt = -1;
        lcda.iSvsMLessF5 = -1;
        lcda.emdist_new = -1;
        lcda.hitcells = -1;

        samlocs.add(lcda);

        splice.longitude = lcda.longitude;
        splice.latitude = lcda.latitude;
        splice.buildingid = lcda.buildingid;
        splice.ilevel = lcda.ilevel;
    }

	private void AnaSam(Map<String, GridData> grids, MrSplice splice, LocSpliceMidInfo splicemidinfo, int ntype)
	{
		if (splice.scell == null)
		{
			return;
		}

		int lcisdistlimit = 0;
		if ((splice.eci == splicemidinfo.ieci) && (splice.MmeUeS1apId == splicemidinfo.is1apid)
				&& (splice.section_btime == splicemidinfo.isection_btime))
		{
			if (splicemidinfo.ilongitude > 0)
			{
				lcisdistlimit = 1;
			}
		}

		// 20170311 cai的需求一个小区对多个楼宇
		//int bid = GetBuildId(splice);
		String bid = GetBuildId(splice);
		double sseg = GetSegOffset(splice.scell.cell.rsrp_avg / splice.scell.cell.rsrp_cnt);

		LocaData lcda = null;
		Locmidinfo lcMidInfo_in = new Locmidinfo();
		Locmidinfo lcMidInfo_out = new Locmidinfo();

		boolean bFilted = false;
		// 0316 过滤栅格
		// 采样点为室内类型
		if (ntype == 1)
		{
            if (splice.scell.cell.isindoor != 1)
            {
			    if (splice.scell.cell.rsrp_avg != -1000000 && splice.scell.cell.rsrp_cnt > 0)
			    {
			        if (splice.scell.cell.rsrp_avg / splice.scell.cell.rsrp_cnt <= -100)
			        {                        
			        	bFilted = true;
			        }
		        }
            }
		}
		// 这里两个小区集都排过序了
		for (Map.Entry<String, GridData> kp : grids.entrySet())
		{
			GridData grid = kp.getValue();

			if (bFilted)
			{
				if (grid.hasindoor == 1)
				{
					continue;
				}
			    // 栅格所在的楼宇有室分，则跳过
			    if (grid.bid > 0)
			    {
			        if (cInfo.cfgbuilds.containsKey(grid.bid))
			        {
			        	grid.hasindoor = 1;
			        	  
			            continue;
			        }
			    }
			}	
			// 可以改进的地方：这里如果grids是按照经纬度排序的，则可以记录上一次的定位点，按照向前，向后两个方向找，不用找全部的格子
			if (lcisdistlimit == 1) // 有与上一次的距离限制
			{
				if ((Math.abs(grid.ilongitude - splicemidinfo.ilongitude) > 200 * 100)
						|| (Math.abs(grid.ilatitude - splicemidinfo.ilatitude) > 200 * 90)) // 距离设置为200米
				{
					continue;
				}
			}

			// 判断栅格是否为需要的
			if (!GridIsNeed(grid.bid, bid))
			{
				continue;
			}

			LocaData ld;
			try
			{
				ld = AnaGrid(sseg, splice, grid, lcMidInfo_in, lcMidInfo_out);
				if (ld == null)
				{
					continue;
				}
			}
			catch (Exception e)
			{
				//e.printStackTrace(); 
				LOGHelper.GetLogger().writeLog(LogType.info, "AnaSam err " + e.getStackTrace());
				continue;
			}

			// 最后过滤
			lcda = ld;
			lcMidInfo_in.CopyLoc(lcMidInfo_out);
		}

		if (lcda != null)
		{
			lcda.isamebtscnt = lcMidInfo_in.iSameBtsHitCount;
			lcda.iSvsMLessF5 = lcMidInfo_in.iSvsMLessF5;
			lcda.emdist_new = lcMidInfo_in.emdist;
			lcda.hitcells = lcMidInfo_in.iHitCellCount;
			lcda.isamplecnt = splice.scell.cell.rsrp_cnt;

			// add by yht
			if (lcMidInfo_in.iSameBtsHitCount > 0 || lcMidInfo_in.iHitCellCount > 3)
			{
				splicemidinfo.ilongitude = lcda.longitude;
				splicemidinfo.ilatitude = lcda.latitude;
				splicemidinfo.ibuildingid = lcda.buildingid;
				splicemidinfo.ilevel = lcda.ilevel;
			}
			splicemidinfo.ieci = lcda.eci;
			splicemidinfo.is1apid = lcda.s1apid;
			splicemidinfo.isection_btime = lcda.section_btime;

			samlocs.add(lcda);

			splice.longitude = lcda.longitude;
			splice.latitude = lcda.latitude;
			splice.buildingid = lcda.buildingid;
			splice.ilevel = lcda.ilevel;
		}
		else
		{
			// 没有定位上
			lcda = new LocaData();

			if ((splice.MmeUeS1apId == splicemidinfo.is1apid) 
					&& (splice.section_btime == splicemidinfo.isection_btime)
					&& (splice.eci == splicemidinfo.ieci))
			{
				lcda.buildingid = splicemidinfo.ibuildingid;
				lcda.longitude = splicemidinfo.ilongitude;
				lcda.latitude = splicemidinfo.ilatitude;
				lcda.ilevel = splicemidinfo.ilevel;
			}
			else
			{
				lcda.buildingid = -1;
				lcda.longitude = 0;
				lcda.latitude = 0;
				lcda.ilevel = -1;
				// 室分的赋值为小区经纬度
				if (splice.scell.cell.isindoor == 1)
				{
					//lcda.buildingid = bid;
					lcda.longitude = (int) (splice.scell.cell.longitude * 10000000);
					lcda.latitude = (int) (splice.scell.cell.latitude * 10000000);
					lcda.ilevel = 0;
				}
			}

			lcda.s1apid = splice.MmeUeS1apId;
			lcda.eci = splice.eci;
			lcda.rsrp = (splice.scell.cell.rsrp_avg / splice.scell.cell.rsrp_cnt);
			lcda.property = splice.property;
			lcda.doortype = splice.doortype;
			lcda.section_btime = splice.section_btime;// 段落开始时间
			lcda.section_etime = splice.section_etime;// 段落结束时间
			lcda.splice_btime = splice.splice_btime; // 切片开始时间
			lcda.splice_etime = splice.splice_etime; // 切片结束时间
			lcda.isamplecnt = splice.scell.cell.rsrp_cnt;
			lcda.isamebtscnt = -1;
			lcda.iSvsMLessF5 = -1;
			lcda.emdist_new = -1;
			lcda.hitcells = -1;

			samlocs.add(lcda);

			splice.longitude = lcda.longitude;
			splice.latitude = lcda.latitude;
			splice.buildingid = lcda.buildingid;
			splice.ilevel = lcda.ilevel;
		}
	}
	// 20170311 cai的需求一个小区对多个楼宇
	//	private int GetBuildId(MrSplice splice)
	//	{
	//		if ((splice.doortype == 1) || (splice.doortype == 3))
	//		{
	//            if (splice.scell.cell.isindoor == 1)
	//            {
	//                return cInfo.GetBuildingid(splice.scell.eci);
	//            }
	//            else
	//            {
	//				if (splice.incell != null)
	//				{
	//					return cInfo.GetBuildingid(splice.incell.cell.neci);
	//				}
	//            }
	//		}
	//
	//		return -1;
	//	}
	private String GetBuildId(MrSplice splice)
	{
		if ((splice.doortype == 1) || (splice.doortype == 3))
		{
            if (splice.scell.cell.isindoor == 1)
            {
                return cInfo.GetBuildingid(splice.scell.eci);
            }
            else
            {
				if (splice.incell != null)
				{
					return cInfo.GetBuildingid(splice.incell.cell.neci);
				}
            }
		}

		return "#-1#";
	}

	// 20170311 cai的需求一个小区对多个楼宇
	//	private boolean GridIsNeed(int gid, int bid)
	//	{
	//		if (bid != -1)
	//		{
	//			if (bid != gid)
	//			{
	//				return false;
	//			}
	//		}
	//		return true;
	//	}
	private boolean GridIsNeed(int gid, String bid)
	{
		if (!bid.equals("#-1#"))
		{
			return (bid.indexOf("#" + String.valueOf(gid) + "#") >= 0);			
		}		
		return true;
	}
	/*
	 * 处理逻辑：函数传入数据： 外部最后一个确定的格子的：同站小区数\hit小区数\仿真比MR小的数\最后的emdist 处理步骤如下： 1
	 * 服务小区距离差满足： WHEN rsrp_max >=-70 and 天线挂高<=12 THEN 120 WHEN rsrp_max >=-70
	 * and 天线挂高<=22 THEN 220 WHEN rsrp_max >=-70 THEN 300 WHEN rsrp_max >=-80
	 * and 天线挂高<=12 THEN 200 WHEN rsrp_max >=-80 THEN 300 WHEN rsrp_max >=-90
	 * and 天线挂高<=12 THEN 300 否则返回false； 2 服务小区满足： a 仿真-MRavg>=20 or
	 * 仿真-MRavg<=-15 b 仿真在-70~-100之间 且 仿真-MRavg<=-10 满足任意一个，返回false； 3
	 * 配对统计同站小区数，小于输入的同站小区数，返回false； 4 邻区中，有存在两个：仿真-MRavg>=20 or 仿真-MRavg<=-15
	 * ，或者是-70~-100之间 且 仿真-MRavg<=-10，返回false；(这个暂时去掉) 5
	 * 统计hit小区数，hit小区数<输入的hit小区数，返回false； 6 统计hit小区中，仿真比MR小5dB的数，如果该数>1 and
	 * 该数>输入的数，则返回false； 7 统计欧式距离进行对比： a 如果同站小区数>=3 则计算同站小区的平均偏离既可； b 如果同站小区数=2
	 * 则计算同站小区偏离+非同站小区平均偏离 c 如果没有同站小区，则计算非同站小区平均偏离
	 */

	private LocaData AnaGrid(double sseg, MrSplice sam, GridData grid, Locmidinfo midinfo_in, Locmidinfo midinfo_out)
	{
		midinfo_out.init();
		// 找格子中主服
		try
		{
			if (sam == null || grid == null)
			{
				return null;
			}
			if (sam.scell==null )//|| grid.scell == null)
			{
				// 格子无主服
				return null;
			}
		}
		catch (Exception e)
		{
			return null;
		}

		// add by yht begin
		// 1 服务小区距离差限制,室外站才判断
        if (sam.scell.cell.isindoor == 0)
        { 
        	if (sam.scell.cell.rsrp_cnt == 0)
        	{
        		return null;
        	}
			if (sam.scell.cell.isevdislimit < 10000)
			{
				if (grid.scell != null)
				{
					if (((grid.scell.rsrp - sam.scell.cell.rsrp_avg / sam.scell.cell.rsrp_cnt >= 20) && (sam.scell.cell.rsrp_avg / sam.scell.cell.rsrp_cnt > -110))
							|| (Math.abs(sam.scell.cell.latitude * 10000000 - grid.ilatitude) > sam.scell.cell.isevdislimit	* 90))
					{
						return null;
					}
				}
			}	
	
			// 2 服务小区场强限制
			if (grid.scell != null)
			{		
				if ((grid.scell.rsrp - sam.scell.cell.rsrp_avg / sam.scell.cell.rsrp_cnt >= 15)
						|| (grid.scell.rsrp - sam.scell.cell.rsrp_avg / sam.scell.cell.rsrp_cnt <= -10))
				{
					return null;
				}
			}
			//		if (sam.scell.cell.rsrp_avg / sam.scell.cell.rsrp_cnt >= -70
			//				&& sam.scell.cell.rsrp_avg / sam.scell.cell.rsrp_cnt <= -100
			//				&& grid.scell.rsrp - sam.scell.cell.rsrp_avg / sam.scell.cell.rsrp_cnt <= -10)
			//		{
			//			return null;
			//		}
        }
		// add by yht end

		LocaData ld = new LocaData();
		// 查找共有邻区
		int lclastbtsid = -1;
		int lclastSamebtsSet = 0;
		List<Pair<MrPoint, SimuData>> cellpairs = new ArrayList<Pair<MrPoint, SimuData>>();

		for (int ii = 0; ii < sam.cells.size(); ii++)
		{
			// 邻区距离限制
			if ((sam.cells.get(ii).cell.isevdislimit < 10000) && (sam.cells.get(ii).cell.longitude > 0))
			{
				if ((Math.abs(sam.cells.get(ii).cell.longitude * 10000000 - grid.ilongitude) > sam.cells.get(ii).cell.isevdislimit * 100)
						|| (Math.abs(sam.cells.get(ii).cell.latitude * 10000000 - grid.ilatitude) > sam.cells.get(ii).cell.isevdislimit * 90))
				{
					return null;
				}
			}

			if (sam.cells.get(ii).cell.isscell == 1)
			{
				midinfo_out.iHitCellCount++;
				if (lclastbtsid == sam.cells.get(ii).cell.btsid)
				{
					if (lclastSamebtsSet == 0) // 上一次没统计
					{
						midinfo_out.iSameBtsHitCount = midinfo_out.iSameBtsHitCount + 2;
						lclastSamebtsSet = 1;
					}
					else // 上一次统计了
					{
						midinfo_out.iSameBtsHitCount = midinfo_out.iSameBtsHitCount + 1;
					}
				}
				else
				{
					lclastbtsid = sam.cells.get(ii).cell.btsid;
					lclastSamebtsSet = 0;
				}
				// 统计仿真比MR还低-5dB以上的个数
				if (grid.scell != null)
				{
					if ((grid.scell.rsrp - sam.scell.cell.rsrp_avg / sam.scell.cell.rsrp_cnt) < -5)
					{
						if (midinfo_out.iSvsMLessF5 > 100)
						{
							midinfo_out.iSvsMLessF5 = 1;
						}
						else
						{
							midinfo_out.iSvsMLessF5++;
						}
					}
				}
				if (grid.scell != null)
				{
					cellpairs.add(new Pair<MrPoint, SimuData>(sam.cells.get(ii), grid.scell));
				}
				continue;
			}
			SimuData lcSimuData = null;
			if (grid.cells.containsKey(sam.cells.get(ii).cell.cell))
			{
				lcSimuData = grid.cells.get(sam.cells.get(ii).cell.cell);

				if (lclastbtsid == sam.cells.get(ii).cell.btsid)
				{
					if (lclastSamebtsSet == 0) // 上一次没统计
					{
						midinfo_out.iSameBtsHitCount = midinfo_out.iSameBtsHitCount + 2; // 统计同站小区数
						lclastSamebtsSet = 1;
					}
					else // 上一次统计了
					{
						midinfo_out.iSameBtsHitCount = midinfo_out.iSameBtsHitCount + 1;
					}
				}
				else
				{
					lclastbtsid = sam.cells.get(ii).cell.btsid;
					lclastSamebtsSet = 0;
				}
				// 统计仿真比MR还低-5dB以上的个数
				if ((lcSimuData.rsrp - sam.cells.get(ii).cell.rsrp_avg / sam.cells.get(ii).cell.rsrp_cnt) < -5)
				{
					if (midinfo_out.iSvsMLessF5 > 100)
					{
						midinfo_out.iSvsMLessF5 = 1;
					}
					else
					{
						midinfo_out.iSvsMLessF5++;
					}
				}

				ld.hitcells++;

				midinfo_out.iHitCellCount++; // 统计hit小区数

				cellpairs.add(new Pair<MrPoint, SimuData>(sam.cells.get(ii), lcSimuData));
			}
		}

		int lcisbetter = 0;
		// 3 配对同站小区数统计
		if (midinfo_out.iSameBtsHitCount < midinfo_in.iSameBtsHitCount)
		{
			return null;
		}
		else if (midinfo_out.iSameBtsHitCount > midinfo_in.iSameBtsHitCount)
		{
			lcisbetter = 1;
		}

		// 4 统计hit小区数，hit小区数<输入的hit小区数，返回false；
		if (midinfo_out.iHitCellCount >= 6)
		{
			midinfo_out.iHitCellCount = midinfo_out.iHitCellCount - 1;
		}
		if (midinfo_out.iHitCellCount > 8)
		{
			midinfo_out.iHitCellCount = 8;
		}
		if (lcisbetter == 0)
		{
			if (midinfo_out.iHitCellCount < midinfo_in.iHitCellCount)
			{
				return null;
			}
			else if (midinfo_out.iHitCellCount > midinfo_in.iHitCellCount)
			{
				lcisbetter = 1;
			}
		}

		// 5 统计hit小区中，仿真比MR小5dB的数，如果该数>1 and 该数>输入的数，则返回false
		if (lcisbetter == 0)
		{
			if (midinfo_out.iSvsMLessF5 > 1 && midinfo_out.iSvsMLessF5 > midinfo_in.iSvsMLessF5)
			{
				return null;
			}
			else if (midinfo_in.iSvsMLessF5 > 1 && midinfo_in.iSvsMLessF5 > midinfo_out.iSvsMLessF5)
			{
				lcisbetter = 1;
			}
		}

		// 6 接下来就是计算欧氏距离了
		if (midinfo_out.iSameBtsHitCount >= 3)
		{
			// 仅计算同站距离既可
			for (int ii = 0; ii < cellpairs.size() - 1; ii++)
			{
				int lcstart = ii; // 同站的起始点和结束点
				int lcend = ii;// 同站的起始点和结束点
				int jj = ii;
				for (jj = ii; jj < cellpairs.size() - 1; jj++)
				{
					if (cellpairs.get(jj).getKey().cell.btsid == cellpairs.get(jj + 1).getKey().cell.btsid)
					{
						lcend++;
					}
					else
					{
						break;
					}
				}
				if (lcend > lcstart) // 有同站数据，进行计算
				{
					// 先计算平均偏差
					double lcdiff = 0;
					double lcemdiff = 0;
					for (int kk = lcstart; kk <= lcend; kk++)
					{
						lcdiff = lcdiff + (cellpairs.get(kk).getValue().rsrp - (cellpairs.get(kk).getKey().cell.rsrp_avg / cellpairs.get(kk).getKey().cell.rsrq_cnt));
					}
					lcdiff = lcdiff / (lcend - lcstart + 1);
					for (int kk = lcstart; kk <= lcend; kk++)
					{
						lcemdiff = lcemdiff + Math.abs(cellpairs.get(kk).getValue().rsrp - (cellpairs.get(kk).getKey().cell.rsrp_avg / cellpairs.get(kk).getKey().cell.rsrq_cnt)	- lcdiff);
					}
					// lcemdiff线不求平均，回头再求平均
					if (midinfo_out.emdist >= 1000)
					{
						midinfo_out.emdist = lcemdiff;
					}
					else
					{
						midinfo_out.emdist = midinfo_out.emdist + lcemdiff;
					}
				}

				ii = jj;

			}
			// 取平均值
			midinfo_out.emdist = midinfo_out.emdist / midinfo_out.iSameBtsHitCount;
		}
		else // if (midinfo_out.iHitCellCount == 2)
		{
			// 计算同站距离和其它距离
			// 先计算平均偏差
			double lcdiff_samebts = 0;
			double lcdiff_others = 0;
			int lcdiff_samebtsflag = 0; // 记录是否已经同站记录过了
			double lcemdiff_samebts = 0.0;
			double lcemdiff_others = 0.0;
			// double lcemdiff = 0.0;
			for (int ii = 0; ii < cellpairs.size() - 1; ii++)
			{
				// 判断是否同站，同站的计算到一起
				if (cellpairs.get(ii).getKey().cell.btsid == cellpairs.get(ii + 1).getKey().cell.btsid)
				{
					lcdiff_samebts = (cellpairs.get(ii).getValue().rsrp - (cellpairs.get(ii).getKey().cell.rsrp_avg / cellpairs.get(ii).getKey().cell.rsrp_cnt))
							+ (cellpairs.get(ii + 1).getValue().rsrp - (cellpairs.get(ii + 1).getKey().cell.rsrp_avg / cellpairs.get(ii + 1).getKey().cell.rsrp_cnt));
					lcdiff_samebtsflag = 1;
					lcdiff_samebts = lcdiff_samebts / midinfo_out.iSameBtsHitCount;
					lcemdiff_samebts = Math.abs((cellpairs.get(ii).getValue().rsrp - (cellpairs.get(ii).getKey().cell.rsrp_avg / cellpairs.get(ii).getKey().cell.rsrq_cnt)) - lcdiff_samebts)
							+ Math.abs((cellpairs.get(ii + 1).getValue().rsrp - (cellpairs.get(ii + 1).getKey().cell.rsrp_avg / cellpairs.get(ii + 1).getKey().cell.rsrq_cnt)) - lcdiff_samebts);
				}
				else if (lcdiff_samebtsflag == 0) // 没有被记录过，则记录到其它中
				{
					lcdiff_others = lcdiff_others + (cellpairs.get(ii).getValue().rsrp - (cellpairs.get(ii).getKey().cell.rsrp_avg / cellpairs.get(ii).getKey().cell.rsrq_cnt));
					lcdiff_samebtsflag = 0;
				}
			}
			if (lcdiff_samebtsflag == 0) // 需要处理最后一个
			{
				lcdiff_others = lcdiff_others + (cellpairs.get(cellpairs.size() - 1).getValue().rsrp - (cellpairs.get(cellpairs.size() - 1).getKey().cell.rsrp_avg / cellpairs.get(cellpairs.size() - 1).getKey().cell.rsrq_cnt));
			}
			if (midinfo_out.iHitCellCount - midinfo_out.iSameBtsHitCount > 0)
			{
				lcdiff_others = lcdiff_others / (midinfo_out.iHitCellCount - midinfo_out.iSameBtsHitCount);
				// 下面计算非同站的偏差
				lcdiff_samebtsflag = 0;
				for (int ii = 0; ii < cellpairs.size() - 1; ii++)
				{
					if (cellpairs.get(ii).getKey().cell.btsid == cellpairs.get(ii + 1).getKey().cell.btsid)
					{
						lcdiff_samebtsflag = 1;
					}
					else if (lcdiff_samebtsflag == 0)
					{
						lcemdiff_others = lcemdiff_others + Math.abs((cellpairs.get(ii).getValue().rsrp - (cellpairs.get(ii).getKey().cell.rsrp_avg / cellpairs.get(ii).getKey().cell.rsrq_cnt))- lcdiff_others);
						lcdiff_samebtsflag = 0;
					}
				}
				if (lcdiff_samebtsflag == 0) // 需要处理最后一个
				{
					lcemdiff_others = lcemdiff_others + Math.abs((cellpairs.get(cellpairs.size() - 1).getValue().rsrp 
							- (cellpairs.get(cellpairs.size() - 1).getKey().cell.rsrp_avg / cellpairs.get(cellpairs.size() - 1).getKey().cell.rsrq_cnt)) - lcdiff_others);
				}
			}
			else
			{
				// 异常
				lcdiff_others = 0;
			}
			midinfo_out.emdist = (lcemdiff_others + lcemdiff_samebts) / midinfo_out.iHitCellCount;
		}

		if (lcisbetter == 0)
		{
			if (midinfo_out.emdist >= midinfo_in.emdist)
			{
				return null;
			}
		}

		ld.emdist_new = midinfo_out.emdist;

		ld.s1apid = sam.MmeUeS1apId;
		ld.eci = sam.eci;
		ld.rsrp = (sam.scell.cell.rsrp_avg / sam.scell.cell.rsrp_cnt);
		ld.property = sam.property;
		ld.doortype = sam.doortype;
		ld.buildingid = grid.bid;
		ld.longitude = grid.ilongitude;
		ld.latitude = grid.ilatitude;
		ld.ilevel = grid.level;
		ld.section_btime = sam.section_btime;// 段落开始时间
		ld.section_etime = sam.section_etime;// 段落结束时间
		ld.splice_btime = sam.splice_btime; // 切片开始时间
		ld.splice_etime = sam.splice_etime; // 切片结束时间

		return ld;
	}

	private double GetSegOffset(double rsrp)
	{
		// 注：区间分类，采用分段对比：
		// Seg1：（~-75]：优= 1
		// Seg2：（-75~-83]：优良过渡 = 1.5
		// Seg3：（-83~-93]：良 = 2
		// Seg4：（-93~-100]：良中过渡= 2.5
		// Seg4：（-100~-107]：中= 3
		// Seg5：（-107~-113]：中差过渡= 3.5
		// Seg6：（-113~）：差= 4
		int nn = (int) (rsrp * (-1));
		if (nn <= 113)
		{
			return r_offset[nn];
		}
		return 4;
	}

	private void InitOffset()
	{
		for (int ii = 0; ii <= 113; ii++)
		{
			if (ii <= 75)
			{
				r_offset[ii] = 1;
			}
			else if (ii <= 83)
			{
				r_offset[ii] = 1.5;
			}
			else if (ii <= 93)
			{
				r_offset[ii] = 2;
			}
			else if (ii <= 100)
			{
				r_offset[ii] = 2.5;
			}
			else if (ii <= 107)
			{
				r_offset[ii] = 3;
			}
			else if (ii <= 113)
			{
				r_offset[ii] = 3.5;
			}
		}
	}
}
