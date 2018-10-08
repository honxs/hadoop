package cn.mastercom.bigdata.evt.locall.stat;

import cn.mastercom.bigdata.util.ResultOutputer;

/**
 * 用来统计和输出的controller类
 * @author zhaikaishun
 */
public class DataStater
{
	private DayDataDeal_4G dayDataDeal4G;
    
    public DataStater(ResultOutputer resultOutputer)
    {
    	dayDataDeal4G = new DayDataDeal_4G(resultOutputer);
    	 
    }
	
	public int stat(EventData eventData)
	{
		dayDataDeal4G.dealEvent(eventData);
		return 0;
	}
	
	public int outResult()
	{
		// ALL
		dayDataDeal4G.outResult();

		return 0;
	}
	
	
}
