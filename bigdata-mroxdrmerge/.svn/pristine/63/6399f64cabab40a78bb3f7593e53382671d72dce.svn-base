package cn.mastercom.bigdata.spark.mergestat;

import cn.mastercom.bigdata.util.ClassHelper;


public class MergeStat
{
	private MergeTypeInfo mergeTypeInfo;
	private IMergeDo resultItem;

	public MergeStat(MergeTypeInfo mergeTypeInfo)
	{
		this.mergeTypeInfo = mergeTypeInfo;
	}

	public void deal(Iterable<String> dataList)
	{
		for (String strtemp : dataList)
		{
			IMergeDo item = (IMergeDo)ClassHelper.GetClass(mergeTypeInfo.mergeClassName);
			item.fillData(strtemp);
			
			if(resultItem == null)
			{
				resultItem = item;
			}
			else 
			{
				resultItem.mergeData(item);
			}
		}
	}

	public String outResult()
	{
		return resultItem.getData();
	}

}