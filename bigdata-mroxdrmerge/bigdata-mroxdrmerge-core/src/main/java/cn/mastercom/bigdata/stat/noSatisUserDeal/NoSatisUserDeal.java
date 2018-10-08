package cn.mastercom.bigdata.stat.noSatisUserDeal;

import java.text.SimpleDateFormat;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.mro.loc.XdrLabel;
import cn.mastercom.bigdata.mro.loc.XdrLabelMng;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class NoSatisUserDeal
{
	private ResultOutputer resultOutputer;
	private XdrLabelMng xdrLableMng;
	private static SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public NoSatisUserDeal(ResultOutputer resultOutputer)
	{
		this.resultOutputer = resultOutputer;
	}
	public void deal(Iterable<Text> xdrValues, Iterable<Text> gmosWjtdhValues)
	{
		if (xdrValues != null)
		{
			xdrLableMng = new XdrLabelMng();
			for (Text value : xdrValues)
			{
				String[] strs = value.toString().split("\t", -1);
				for (int i = 0; i < strs.length; ++i)
				{
					strs[i] = strs[i].trim();
				}

				XdrLabel xdrLable;
				try
				{
					xdrLable = XdrLabel.FillData(strs, 0);
					xdrLableMng.addXdrLocItem(xdrLable);
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error,"XdrLable.FillData error", "XdrLable.FillData error ", e);
					continue;
				}
			}
			xdrLableMng.init();
		}
		if (gmosWjtdhValues != null)
		{
//			Text value;
			String[] strs;
			int outType = 0;
			LOGHelper.GetLogger().writeLog(LogType.info, "begin  GLI_MSI, " + xdrLableMng.getSize());
//			while (gmosWjtdhValues.iterator().hasNext())
			for(Text value : gmosWjtdhValues)
			{
				try
				{
					strs = value.toString().split(",", -1);
					GL_BaseInfo baseInfo = new GL_BaseInfo();
					baseInfo.content = value.toString();
					if (strs.length >= 57)
					{
						baseInfo.times = (int) (sf.parse(strs[0]).getTime() / 1000);
						baseInfo.mmeUes1apid = Long.parseLong(strs[56]);
						outType = 1;
					}
					else if (strs.length >= 11)
					{
						baseInfo.times = (int) (sf.parse(strs[1]).getTime() / 1000);
						baseInfo.mmeUes1apid = Long.parseLong(strs[11]);
						outType = 2;
					}

					if (xdrLableMng.dealGLBaseInfo(baseInfo))
					{
						resultOutputer.pushData(outType, value.toString() + "," + baseInfo.imsi);
					}
				}
				catch (Exception e)
				{
				}
			}
		}
		((ArrayList<Text>)xdrValues).clear();
		((ArrayList<Text>)gmosWjtdhValues).clear();
	}
}
