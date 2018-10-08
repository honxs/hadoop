package cn.mastercom.bigdata.mro.stat;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.util.TimeHelper;

/**
 * @Description 利用传入的策略 按天处理4G数据 <策略模式>
 * @author kwong
 * @date 2017/09/19
 */
public class DayDataDeal_4G
{
	/**
	 * 按天统计的集合
	 */
	private Map<Integer, IStatDo> dayDataDealMap;
	/**
	 * 统计策略
	 */
	private IStatDo statDo;

	public DayDataDeal_4G(IStatDo statDo)
	{
		dayDataDealMap = new HashMap<Integer, IStatDo>();
		Objects.requireNonNull(statDo);
		//注入策略
		this.statDo = statDo;
		
	}

	public void dealSample(DT_Sample_4G sample)
	{
//		if (sample.itime == 0)
//		{
//			return;
//		}

		int curDayTime = TimeHelper.getRoundDayTime(sample.itime);
		StatDoWrapper statDoWapper = (StatDoWrapper)dayDataDealMap.get(curDayTime);
		if (statDoWapper == null)
		{
			statDoWapper = new StatDoWrapper(statDo, curDayTime, curDayTime + 86400);
			dayDataDealMap.put(curDayTime, statDoWapper);
		}
		statDoWapper.dealMr(sample);
	}
	
	public Map<Integer, IStatDo> getDayDataDealMap()
	{
		return dayDataDealMap;
	}

	public int outResult()
	{
		for (IStatDo statDo : dayDataDealMap.values())
		{
			((StatDoWrapper)statDo).outResult();
		}
		return 0;
	}
	
	/**
	 * @Description 封装 策略，以提供更多的行为
	 */
	private class StatDoWrapper implements IStatDo
	{
		private int stime;
		private int etime;
		private IStatDo statDo;

		public StatDoWrapper(IStatDo statDo, int stime, int etime)
		{
			this.stime = stime;
			this.etime = etime;
			this.statDo = statDo;
		}

		public void outResult()
		{
			statDo.outFinalReuslt();
		}

		public void dealMr(DT_Sample_4G sample)
		{
			statDo.stat(sample);
		}

		public void dealEvent(DT_Sample_4G sample)
		{
			statDo.stat(sample);
		}

		@Override
		public int stat(Object sample)
		{
			return statDo.stat(sample);
		}

		@Override
		public int outDealingResult()
		{
			return statDo.outDealingResult();
		}

		@Override
		public int outFinalReuslt()
		{
			return statDo.outFinalReuslt();
		}
	}

}