package cn.mastercom.bigdata.stat.userAna.hsr;

import java.util.ArrayList;
import java.util.List;

import cn.mastercom.bigdata.stat.userAna.model.ImsiInfoSimple;

/*
 * 用车用户信息
 */
public class ImsiGroupInfo
{
	public final long stopSpan = 180000;
	/**
	 * 用户信息
	 */
	public List<ImsiInfoSimple> imsiInfoList;

	/**
	 * 开车时间
	 */
	public long beginTime;
	/**
	 * 到达时间
	 */
	public long endTime;

	/**
	 * 始站是否停车
	 */
	public boolean stopStart = false;

	/**
	 * 末站是否停车
	 */
	public boolean stopEnd = false;

	private RailTrain m_railTrain;

	public ImsiGroupInfo(RailTrain railTrain)
	{
		imsiInfoList = new ArrayList<ImsiInfoSimple>();
		m_railTrain = railTrain;

		if (m_railTrain.stationBegin.outProvince)
		{
			beginTime = Long.MAX_VALUE;
		}
		else
		{
			beginTime = Long.MIN_VALUE;
		}

		if (m_railTrain.stationEnd.outProvince)
		{
			endTime = Long.MIN_VALUE;
		}
		else
		{
			endTime = Long.MAX_VALUE;

		}
	}

	public void add(ImsiInfoSimple imsiInfo)
	{
		imsiInfoList.add(imsiInfo);

		//  取单组用户的起始最大时间和结束最小时间作为该段列车运行的起始时间和终止时间
		//虚拟站点则刚好相反
		if (m_railTrain.stationBegin.outProvince)
		{
			if (beginTime > imsiInfo.beginTime) beginTime = imsiInfo.beginTime;
		}
		else
		{
			if (beginTime < imsiInfo.beginTime) beginTime = imsiInfo.beginTime;

			if (imsiInfo.beginTime - imsiInfo.minBeginTime > stopSpan)
			{
				stopStart = true;
			}
		}

		if (m_railTrain.stationEnd.outProvince)
		{
			if (endTime < imsiInfo.endTime) endTime = imsiInfo.endTime;
		}
		else
		{
			if (endTime > imsiInfo.endTime) endTime = imsiInfo.endTime;

			if (imsiInfo.maxEndTime - imsiInfo.endTime > stopSpan)
			{
				stopEnd = true;
			}
		}

	}

}
