package cn.mastercom.bigdata.mro.stat;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.struct.IStat_4G;
import cn.mastercom.bigdata.util.ResultOutputer;

public abstract class AMapStatDo_4G<T extends IStat_4G> implements IStatDo<DT_Sample_4G>
{
	protected ResultOutputer resultOutputer;
	protected long oneCount = 0;
	protected long oneCountMax = 100000;
	protected int sourceType;
	protected int confidenceType;
	protected int dataType;
	protected Map<String, T> statMap;

	Method statMethod;

	public AMapStatDo_4G(ResultOutputer resultOutputer, int sourceType, int dataType)
	{
		this(resultOutputer, sourceType, StaticConfig.Natural_Abnormal, dataType);
	}

	public AMapStatDo_4G(ResultOutputer resultOutputer, int sourceType, int confidenceType, int dataType)
	{
		this.resultOutputer = resultOutputer;
		this.sourceType = sourceType;
		this.confidenceType = confidenceType;
		this.dataType = dataType;
		statMap = new HashMap<>();
		////////
		try
		{
			if (sourceType == StaticConfig.SOURCE_YD || sourceType == StaticConfig.SOURCE_YDLT || sourceType == StaticConfig.SOURCE_YDDX)
			{
				statMethod = IStat_4G.class.getMethod("doSample", DT_Sample_4G.class);
			}
			else if (sourceType == StaticConfig.SOURCE_DX)
			{
				statMethod = IStat_4G.class.getMethod("doSampleDX", DT_Sample_4G.class);
			}
			else if (sourceType == StaticConfig.SOURCE_LT)
			{
				statMethod = IStat_4G.class.getMethod("doSampleLT", DT_Sample_4G.class);
			}
			else
				throw new IllegalArgumentException();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new RuntimeException();
		}

	}
	public AMapStatDo_4G<T> withStatMap(Map<String, T> statMap) {
		this.statMap = statMap;
		return this;
	}
	@Override
	public int stat(DT_Sample_4G sample)
	{
		if (!MrStatUtil.rsrpRight(sample, sourceType))// 如果rsrp不合法 直接返回
		{
			return 0;
		}
		if (sample.ConfidenceType != this.confidenceType && this.confidenceType != StaticConfig.Natural_Abnormal) // 不是当然的置信度
		{
			return 0;
		}
		if (statOrNot(sample))
		{
			oneCount++;

			Object[] keys = getPartitionKeys(sample);
			String unitkey = StringUtils.join(keys, "_");
			T stat = statMap.get(unitkey);
			if (stat == null)
			{
				stat = createFirstStatItem(sample, keys);
				statMap.put(unitkey, stat);
			}
			deal(sample, stat);
		}
		outDealingResult();
		return 0;
	}

	@Override
	public int outDealingResult()
	{
		if (oneCountMax < oneCount)
		{
			oneCount = 0;
			outFinalReuslt();
		}
		return 0;
	}

	@Override
	public int outFinalReuslt()
	{
		oneCount = 0;
		for (T item : statMap.values())
		{
			try
			{
				resultOutputer.pushData(dataType, item.toLine());
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		statMap.clear();
		return 0;
	}

	public int getIfreq(int sourceType, DT_Sample_4G sample)
	{
		if (sourceType == StaticConfig.SOURCE_YD || sourceType == StaticConfig.SOURCE_YDLT || sourceType == StaticConfig.SOURCE_YDDX)
		{
			return sample.LteScEarfcn;
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			return sample.dx_freq[0].LteNcEarfcn;
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			return sample.lt_freq[0].LteNcEarfcn;
		}
		return 0;
	}

	protected void deal(DT_Sample_4G sample, T stat)
	{
		/* 统计方法在初始化时就被赋值，不用每次判断运营商 */
		try
		{
			statMethod.invoke(stat, sample);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

	protected abstract Object[] getPartitionKeys(DT_Sample_4G sample);

	/**
	 * 创建第一个统计对象
	 * 
	 * @param sample
	 * @param keys
	 *            按什么键分组 {@link getPartitionKeys}
	 * @return
	 */
	protected abstract T createFirstStatItem(DT_Sample_4G sample, Object[] keys);

	protected abstract boolean statOrNot(DT_Sample_4G sample);

}
