package cn.mastercom.bigdata.evt.locall.model;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;

import cn.mastercom.bigdata.base.model.ExternalDO;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;



public abstract class XdrDataBase implements Serializable,Comparable<XdrDataBase>, ExternalDO
{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compareTo(XdrDataBase o)
	{
		double a = this.getIstime() + this.getIstimems() / 1000.0;
		double b = o.getIstime() + o.getIstimems() / 1000.0;

		if (a > b)
		{
			return 1;
		}
		else if (a < b)
		{
			return -1;
		}
		else
		{
			return 0;
		}
	}

	private int dataType;
	
   
	public long imsi;
	public long noEntryImsi;// 用来记录没有加密的imsi
	public int istime;
	public int istimems;
	public int ietime;
	public int ietimems;
	
	public int iCityID = -1;//-1 默认进dispose库
	public int iLongitude;
	public int iLatitude;
	public int iDoorType;
	public int iAreaType;
	public int iAreaID;
	public String strloctp;
	public int iRadius;

	public int ibuildid;
	public int iheight;
	public int testType;
	public String label;
	public int locSource;

	private String srcData;
	
	public int LteScRSRP;
	public int LteScSinrUL;
	
	public int confidentType;
	
	public long s1apid;
	public long ecgi;
	
	public String imei;
	
	//高铁信息
	public long trainKey;
	public int sectionId;
	public int segmentId;

	public int position;	//方位

	public String msisdn_neimeng = "";
	
	public XdrDataBase()
	{
		dataType = -1;
		msisdn_neimeng="";
		imsi = -1;
		istime = -1;
		ietime = -1;
		iLongitude = -1;
		iLatitude = -1;
		iDoorType = -1;
		iAreaType = -1;
		iAreaID = -1;
		strloctp = "";
		iRadius = -1;

		srcData = "";
		
		LteScRSRP = -1000000;
		LteScSinrUL = -1000000;
		
		confidentType = -1;
		imei = "";
		label = "";
	}

	public long getImsi()
	{
		return imsi;
	}

	public int getIstime()
	{
		return istime;
	}
	
	public int getIstimems()
	{
		return istimems;
	}

	public int getIetime()
	{
		return ietime;
	}
	
	public int getIetimems()
	{
		return ietimems;
	}

	/**
	 * 得到当前对象的dataType
	 */
	public int getDataType()
	{
		return dataType;
	}
	
	public void setDataType(int dataType)
	{
		this.dataType = dataType;
	}
	
	
	
	public long getS1apid()
	{
		return s1apid;
	}

	public void setS1apid(long s1apid)
	{
		this.s1apid = s1apid;
	}

	public long getEcgi()
	{
		return ecgi;
	}

	public void setEcgi(long ecgi)
	{
		this.ecgi = ecgi;
	}

	/**
	 * 返回子类的数据接口类型
	 * @return
	 */
	public abstract int getInterfaceCode();

	/**
	 *
	 * @return 返回当前对象解析器
	 * @throws IOException
	 */
	public abstract ParseItem getDataParseItem() throws IOException;

	/**
	 * 
	 * @param dataAdapterReader
	 *  对数据的字段进行提取，FillData_short就是为了少提取一些
	 * @throws ParseException  数据解析异常
	 * @throws IOException
	 */
	public abstract boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException;
	
	public abstract boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException;
	
	/**
	 * 对XdrDataBase的数据，如果满足事件的条件，那么输出一个EventData话单    
	 * 可能某种数据会产生很多种指标，一个EventData放不下，所以会有多个EventData，返回的是一个ArrayList<EventData>  
	 * 统计的事件放在  EventData.eventStat中，一般存放统计指标，在后面会进行汇聚统计等操作。    
	 * 详单数据放在EventData.eventDetial中，一般用来存放某一条数据的某些结果，不会进行汇聚统计等操作  
	 * @see EventData EventDataStruct
	 * @return
	 */
    public abstract ArrayList<EventData> toEventData();
    
    public abstract void toString(StringBuffer sb);

	@Override
	public boolean fillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException {
		return FillData(dataAdapterReader);
	}

	@Override
	public void fromFormatedString(String value) throws Exception {

	}

	@Override
	public String toFormatedString() {
		return null;
	}
}
