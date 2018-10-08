package cn.mastercom.bigdata.xdr.loc;

import java.util.List;

import cn.mastercom.bigdata.util.GisFunction;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.StructData.SIGNAL_LOC;
import cn.mastercom.bigdata.StructData.StaticConfig;

public class TestTypeDeal
{
	private long imsi;
	private boolean isDDDriver;

	/**
	 * 可回填DT的最大时间差
	 */
	private static final int DT_FILL_TIME_SPAN_MAX = 10;
	/**
	 * 可回填DTEX的最大时间差
	 */
	private static final int DTEX_FILL_TIME_SPAN_MAX = 10;
	/**
	 * 可回填CQT的最大位移
	 */
	private static final int CQT_FILL_DIST_MAX = 100;

	// private Map<String, TimeSpan> userHomeCellMap = new HashMap<String,
	// TimeSpan>();

	public TestTypeDeal(long imsi, boolean isDDDriver)
	{
		this.imsi = imsi;
		this.isDDDriver = isDDDriver;
		// this.userHomeCellMap = userHomeCellMap;
	}

	public long getImsi()
	{
		return imsi;
	}

	public void deal(List<? extends SIGNAL_LOC> xdrItemList)
	{
		if (isDDDriver)
		{
			LOGHelper.GetLogger().writeLog(LogType.info, "find didi driver : " + imsi);

			for (int i = 0; i < xdrItemList.size(); ++i)
			{
				SIGNAL_LOC xitem = xdrItemList.get(i);

				if ((xitem.location == 3)
						|| ((xitem.location == 2 || xitem.location == 4 || xitem.location == 5 || xitem.location == 6 || xitem.location == 9/*有经纬度MDT的location=9*/) && (xitem.radius <= 100 && xitem.radius >= 0 && xitem.longitude > 0)
						&& (xitem.loctp.equals("wf") || xitem.loctp.equals("cl") || xitem.loctp.equals("ll") || xitem.loctp.equals("ll2") || xitem.loctp.equals("lll"))))
				{
					xitem.testType = StaticConfig.TestType_DT;
					fillJoinFields(xitem, xitem);

					SIGNAL_LOC jitem = null;
					// 往前找
					int curJ = i - 1;
					while (curJ >= 0)
					{
						jitem = xdrItemList.get(curJ);
						//仅回填 当前时间更接近的采样点
						int span = Math.abs(xitem.stime - jitem.stime);
						if (span <= DT_FILL_TIME_SPAN_MAX && jitem.longitude <= 0 && span < Math.abs(xitem.stime - jitem.loctimeGL))
						{
							fillJoinFields(xitem, jitem);
						}
						else
						{
							break;
						}
						curJ--;
					}

					// 往后找
					curJ = i + 1;
					while (curJ < xdrItemList.size())
					{
						jitem = xdrItemList.get(curJ);
						//仅回填 当前时间更接近的采样点
						int span = Math.abs(xitem.stime - jitem.stime);
						if (span <= DT_FILL_TIME_SPAN_MAX && jitem.longitude <= 0 && span < Math.abs(xitem.stime - jitem.loctimeGL))
						{
							fillJoinFields(xitem, jitem);
						}
						else
						{
							break;
						}
						curJ++;
					}
				}
			}
		}
		else // 如果发现没有dt的点，但是sdk为高速的用户，也需要打上dt的标签
		{
			int cqtTestStartPos = -1;
			int cqtTestEndPos = -1;

			for (int i = 0; i < xdrItemList.size(); i++)
			{
				SIGNAL_LOC xitem = xdrItemList.get(i);
				SIGNAL_LOC jitem;
				int curJ;

				if (((xitem.location == 2 || xitem.location == 4 || xitem.location == 5 || xitem.location == 7 || xitem.location == 9/*有经纬度MDT的location=9*/) && xitem.radius <= 100 && xitem.radius >= 0 && xitem.longitude > 0)
						|| (xitem.location == 6 && xitem.radius <= 50 && xitem.radius >= 0 && xitem.longitude > 0)
						|| (xitem.mt_label.equals("esti_static") && xitem.location == 10 && (xitem.loctp.equals("ru") || xitem.loctp.equals("hb"))))
				{
					if ((!xitem.mt_label.equals("unknow") && !xitem.mt_label.equals("static")))
					{
						cqtTestStartPos = -1;
						cqtTestEndPos = -1;
					}

					if (xitem.mt_label.equals("high") && (xitem.loctp.equals("wf") || xitem.loctp.equals("ll") || xitem.loctp.equals("ll2") || xitem.loctp.equals("lll")))
					{
						// 高速点回填
						xitem.testType = StaticConfig.TestType_DT;
						fillJoinFields(xitem, xitem);

						// 往前找
						curJ = i - 1;
						while (curJ >= 0)
						{
							jitem = xdrItemList.get(curJ);
							//仅回填 当前时间更接近的采样点
							int span = Math.abs(xitem.stime - jitem.stime);
							if (span <= DT_FILL_TIME_SPAN_MAX && jitem.longitude <= 0 && span < Math.abs(xitem.stime - jitem.loctimeGL))
							{
								fillJoinFields(xitem, jitem);
							}
							else
							{
								break;
							}
							curJ--;
						}

						// 往后找
						curJ = i + 1;
						while (curJ < xdrItemList.size())
						{
							jitem = xdrItemList.get(curJ);
							//仅回填 当前时间更接近的采样点
							int span = Math.abs(xitem.stime - jitem.stime);
							if (span <= DT_FILL_TIME_SPAN_MAX && jitem.longitude <= 0 && span < Math.abs(xitem.stime - jitem.loctimeGL))
							{
								fillJoinFields(xitem, jitem);
							}
							else
							{
								break;
							}
							curJ++;
						}
					}
					else if (xitem.mt_label.equals("low") && (xitem.loctp.equals("wf") || xitem.loctp.equals("ll") || xitem.loctp.equals("ll2") || xitem.loctp.equals("lll")))
					{// 低速点回填
						xitem.testType = StaticConfig.TestType_DT_EX;
						fillJoinFields(xitem, xitem);

						// 往前找
						curJ = i - 1;
						while (curJ >= 0)
						{
							jitem = xdrItemList.get(curJ);
							//仅回填 当前时间更接近的采样点
							int span = Math.abs(xitem.stime - jitem.stime);
							if (span <= DTEX_FILL_TIME_SPAN_MAX && jitem.longitude <= 0 && span < Math.abs(xitem.stime - jitem.loctimeGL))
							{
								fillJoinFields(xitem, jitem);
							}
							else
							{
								break;
							}
							curJ--;
						}

						// 往后找
						curJ = i + 1;
						while (curJ < xdrItemList.size())
						{
							jitem = xdrItemList.get(curJ);
							//仅回填 当前时间更接近的采样点
							int span = Math.abs(xitem.stime - jitem.stime);
							if (span <= DTEX_FILL_TIME_SPAN_MAX && jitem.longitude <= 0 && span < Math.abs(xitem.stime - jitem.loctimeGL))
							{
								fillJoinFields(xitem, jitem);
							}
							else
							{
								break;
							}
							curJ++;
						}
					}
					else if (xitem.radius <= 80 && xitem.radius >= 0 && xitem.mt_label.equals("static")
							&& (xitem.loctp.equals("ll") || xitem.loctp.equals("wf") || xitem.loctp.equals("ll2") || xitem.loctp.equals("lll")))
					{// 静态点回填
						xitem.testType = StaticConfig.TestType_CQT;
						fillJoinFields(xitem, xitem);

						if (cqtTestStartPos >= 0)
						{
							cqtTestEndPos = i;

							SIGNAL_LOC sItem = xdrItemList.get(cqtTestStartPos);
							SIGNAL_LOC eItem = xdrItemList.get(cqtTestEndPos);

							// // 属于常驻小区才能进行回填
							// TimeSpan timeSpan1 =
							// userHomeCellMap.get(sItem.GetCellKey());
							// TimeSpan timeSpan2 = userHomeCellMap.get(eItem.GetCellKey());
							if (true)
							{
								double dis = GisFunction.GetDistance(sItem.longitude, sItem.latitude, eItem.longitude, eItem.latitude);

								if (dis <= CQT_FILL_DIST_MAX)
								{
									for (int ii = cqtTestStartPos + 1; ii < cqtTestEndPos; ++ii)
									{
										jitem = xdrItemList.get(ii);

										if (jitem.longitude <= 0 && jitem.GetCellKey().equals(sItem.GetCellKey()))
										{
											fillJoinFields(sItem, jitem);
										}
										else if (jitem.longitude <= 0 && jitem.GetCellKey().equals(eItem.GetCellKey()))
										{
											fillJoinFields(eItem, jitem);
										}
									}
								}
								else
								{
									// 如果两个静止点相隔太远了，就说明用户这段时间不是静止
									sItem.testType = StaticConfig.TestType_DT_EX;
									sItem.testTypeGL = sItem.testType;
								}
							}

							cqtTestStartPos = cqtTestEndPos;
							cqtTestEndPos = -1;
						}
						else
						{
							cqtTestStartPos = i;
							cqtTestEndPos = -1;
						}
					}
					else if (xitem.location == 10 && (xitem.loctp.equals("ru") || xitem.loctp.equals("hb")) && xitem.mt_label.equals("esti_static"))
					{
						xitem.testType = StaticConfig.TestType_CQT;
						fillJoinFields(xitem, xitem);
					}
				}
			}
		}

		//0601 判断超远的提取到dealSample()中。回填到采样点的部分去掉
		int maxRadius;
		for (int i = 0; i < xdrItemList.size(); i++)
		{
			SIGNAL_LOC tTemp = xdrItemList.get(i);

			// 去除有距离超远的数据
			maxRadius = tTemp.GetMaxCellRadius();
			if (tTemp.distGL >= maxRadius)
			{
				tTemp.testType = StaticConfig.TestType_ERROR;
				continue;
			}

			// 对于用于关联的采样点，测试类型和经纬度，都可以回填，用于用户的回放分析
			if (tTemp.testType != StaticConfig.TestType_DT && tTemp.testType != StaticConfig.TestType_CQT && tTemp.testType != StaticConfig.TestType_DT_EX)
			{
				if (tTemp.testTypeGL == StaticConfig.TestType_DT || tTemp.testTypeGL == StaticConfig.TestType_CQT || tTemp.testTypeGL == StaticConfig.TestType_DT_EX)
				{
					tTemp.longitude = tTemp.longitudeGL;
					tTemp.latitude = tTemp.latitudeGL;

					tTemp.testType = tTemp.testTypeGL;
					tTemp.location = tTemp.locationGL;
					tTemp.dist = tTemp.distGL;
					tTemp.radius = tTemp.radius;
					tTemp.loctp = tTemp.loctpGL;
					tTemp.indoor = StaticConfig.Int_Abnormal;
					tTemp.lable = tTemp.lableGL;
				}
			}

		}

	}

	/**
	 * 回填 用于关联的字段
	 * @param orig
	 * @param targ
	 */
	private void fillJoinFields(SIGNAL_LOC orig, SIGNAL_LOC targ){
		targ.longitudeGL = orig.longitude;
		targ.latitudeGL = orig.latitude;
		targ.testTypeGL = orig.testType;
		targ.locationGL = orig.location;
		targ.distGL = orig.dist;
		targ.radiusGL = orig.radius;
		targ.loctpGL = orig.loctp;
		targ.indoorGL = (int) orig.mt_speed;
		targ.lableGL = orig.mt_label;
		targ.loctimeGL = orig.stime;
		targ.moveDirect = orig.moveDirect;
		//add wifillist for 分层定位
		targ.wifiName = orig.wifiName;

		if (targ != orig && !targ.GetCellKey().equals(orig.GetCellKey()))
		{
			targ.distGL = targ.GetSampleDistance(orig.longitude, orig.latitude);
			if (targ.distGL < 0)
			{
				targ.testType = StaticConfig.TestType_ERROR;
			}
		}
	}

}
