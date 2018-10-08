package cn.mastercom.bigdata.mro.stat;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

/**
 * @Description 多个统计 <组合模式>
 * @author kwong
 * @date 2017/09/19
 */
public class CompositeStatDo implements IStatDo
{
	/**
	 * 一系列统计方法对象
	 */
	private Set<IStatDo> statDoSet;
	
	public CompositeStatDo(Collection<IStatDo> stats)
	{
		if(stats == null || stats.isEmpty()){
			throw new IllegalArgumentException();
		}
		statDoSet = new HashSet<>(stats);
	}
	
	@Override
	public int stat(Object tsam)
	{
		int sum = 0;
		for(IStatDo statDo : statDoSet){
			try {
				sum += statDo.stat(tsam);
			}catch (Exception e){
				e.printStackTrace();
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error, "stat error: " + statDo.getClass().toString());
			}
		}
		return sum;
	}

	@Override
	public int outDealingResult()
	{
		int sum = 0;
		for(IStatDo statDo : statDoSet){
			try{
				sum += statDo.outDealingResult();
			}catch (Exception e){
				e.printStackTrace();
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error, "outDealingReuslt error: " + statDo.getClass().toString());
			}
		}
		return sum;
	}

	@Override
	public int outFinalReuslt()
	{
		int sum = 0;
		for(IStatDo statDo : statDoSet){
			try{
				sum += statDo.outFinalReuslt();
			}catch (Exception e){
				e.printStackTrace();
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error, "outFinalReuslt error: " + statDo.getClass().toString());
			}
		}
		return sum;
	}

}
