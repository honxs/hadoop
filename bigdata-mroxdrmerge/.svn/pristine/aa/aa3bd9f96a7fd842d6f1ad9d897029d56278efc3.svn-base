package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mdt.stat.StatDeal_MDT_IMM;
import cn.mastercom.bigdata.mdt.stat.StatDeal_MDT_LOG;
import cn.mastercom.bigdata.mro.loc.hsr.stat.StatDeal_HSR;
import cn.mastercom.bigdata.util.ResultOutputer;

public class UserMrStat
{
	// private StatDeal statDeal;

	private StatDeals statDeals;
	private StatDeal_HSR statDeal_hsr;
	private StatDeal_MDT_IMM statDeal_mdt_imm;
	private StatDeal_MDT_LOG statDeal_mdt_log;

	public UserMrStat(ResultOutputer typeResult)
	{
		statDeals = new StatDeals(typeResult);
		statDeal_hsr = new StatDeal_HSR(typeResult);
		statDeal_mdt_imm = new StatDeal_MDT_IMM(typeResult);
		statDeal_mdt_log = new StatDeal_MDT_LOG(typeResult);
	}

	public int dealSample(DT_Sample_4G sample)
	{
		if (sample.flag.equals("MRO") || sample.flag.equals("MRE"))
		{
			statDeals.dealSample(sample);
			
			//仅对MRO数据进行高铁统计
			if (sample.testType == StaticConfig.TestType_HiRail && sample.trainKey > 0)
			{
				statDeal_hsr.dealSample(sample);
			}
		}
		else if (sample.flag.equals("MDT_IMM"))
		{
			statDeal_mdt_imm.dealSample(sample);
		}
		else if (sample.flag.equals("MDT_LOG"))
		{
			statDeal_mdt_log.dealSample(sample);
		}

		return 0;
	}

	public int outResult()
	{
		statDeals.outResult();
		statDeal_mdt_imm.outResult();
		statDeal_mdt_log.outResult();
		statDeal_hsr.outResult();
		return 0;
	}

}
