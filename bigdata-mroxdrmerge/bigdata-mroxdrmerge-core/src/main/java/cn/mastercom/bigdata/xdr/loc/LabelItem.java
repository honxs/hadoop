package cn.mastercom.bigdata.xdr.loc;

import java.text.SimpleDateFormat;
import java.util.Date;

import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.*;

public class LabelItem
{
	public long imsi;
	public int begin_time;
	public int end_time;
	public String label;

	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Date d_beginTime;
	private String strTime;

	public boolean FillData(String[] vals, int startPos)
	{
		int i = startPos;
		String imsiStr = DataGeter.GetString(vals[i++], "");
		if (StringUtil.isNum(imsiStr)) {
			imsi = Long.parseLong(imsiStr);
		}
		strTime = vals[i++];
		try
		{
			strTime = strTime.substring(0, strTime.length()-7);
		    d_beginTime = format.parse(strTime);
		    begin_time = (int) (d_beginTime.getTime() / 1000L);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc LabelItem.FillData1 error",
					"xdrloc LabelItem.FillData1 error",	e);
			return false;
		}
		
		strTime = vals[i++];
		try
		{
			strTime = strTime.substring(0, strTime.length()-7);
		    d_beginTime = format.parse(strTime);
		    end_time = (int) (d_beginTime.getTime() / 1000L);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc LabelItem.FillData2 error",
					"xdrloc LabelItem.FillData2 error",	e);
			return false;
		}	

		label = vals[i++];

		return true;
	}
}
