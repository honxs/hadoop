package cn.mastercom.bigdata.util;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import cn.mastercom.bigdata.util.DataAdapterConf.ColumnInfo;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;

public abstract class AAdapterReader implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	protected ParseItem parseItem;
	protected SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
	protected SimpleDateFormat dateFormat3 = new SimpleDateFormat("yyyyMMddHHmmssS");
	protected String tmStr = "";
	
	public AAdapterReader(ParseItem parseItem)
	{
         this.parseItem = parseItem;
	}
	
	public abstract String getValue(ColumnInfo columnInfo);
	
	public int GetIntValue(String columName, int defaultValue) throws ParseException
	{
		ColumnInfo columnInfo = parseItem.getColumInfo(columName);
		tmStr = getValue(columnInfo);
		if(columnInfo == null 
				|| tmStr.length() == 0
				|| tmStr.equals("NIL")
				|| tmStr.equals("null")	
				|| tmStr.equals("NULL")
				|| tmStr.equals("--")
				|| tmStr.equals("\\N"))
		{
			return defaultValue;
		}
			
		return Integer.parseInt(parseData(tmStr, columnInfo.formatFunc).toString());
	}
	
	public long GetLongValue(String columName, long defaultValue) throws ParseException
	{
		ColumnInfo columnInfo = parseItem.getColumInfo(columName);
		tmStr = getValue(columnInfo);
		if(columnInfo == null 
				|| tmStr.length() == 0
				|| tmStr.equals("NIL")
				|| tmStr.equals("null")	
				|| tmStr.equals("NULL")
				|| tmStr.equals("--")
				|| tmStr.equals("\\N"))
		{
			return defaultValue;
		}
		
		return Long.parseLong(parseData(tmStr, columnInfo.formatFunc).toString());
	}
	
	public String GetStrValue(String columName, String defaultValue) throws ParseException
	{
		ColumnInfo columnInfo = parseItem.getColumInfo(columName);
		tmStr = getValue(columnInfo);
		if(columnInfo == null  
				|| tmStr.length() == 0
				|| tmStr.equals("NIL")
				|| tmStr.equals("null")	
				|| tmStr.equals("NULL")
				|| tmStr.equals("--")
				|| tmStr.equals("\\N"))
		{
			return defaultValue;
		}
		return parseData(tmStr, columnInfo.formatFunc).toString().trim();
	}
	
	public Double GetDoubleValue(String columName, double defaultValue) throws ParseException
	{
		ColumnInfo columnInfo = parseItem.getColumInfo(columName);
		tmStr = getValue(columnInfo);
		if(columnInfo == null 
				|| tmStr.length() == 0
				|| tmStr.equals("NIL")
				|| tmStr.equals("null")	
				|| tmStr.equals("NULL")
				|| tmStr.equals("--")
				|| tmStr.equals("\\N"))
		{
			return defaultValue;
		}
	
		return Double.parseDouble(parseData(tmStr, columnInfo.formatFunc).toString());
	}
	
	public Date GetDateValue(String columName, Date defaultValue) throws ParseException
	{
		ColumnInfo columnInfo = parseItem.getColumInfo(columName);
		tmStr = getValue(columnInfo);
		if(columnInfo == null 
				|| tmStr.length() == 0
				|| tmStr.equals("NIL")
				|| tmStr.equals("null")	
				|| tmStr.equals("NULL")
				|| tmStr.equals("--")
				|| tmStr.equals("\\N"))
		{
			return defaultValue;
		}
	
		return (Date)(parseData(tmStr, columnInfo.formatFunc));
	}
	
		/**
	 * �����������ֶ���ƴ�ӳ��ַ�
	 * 
	 * @param coluName
	 *            �ֶ�
	 * @return
	 */
	
	public String getAppendString(List<String> coluName)
	{
		StringBuffer sb = new StringBuffer();
		int i = 0;
		for (String columName : coluName)
		{
			ColumnInfo columnInfo = parseItem.getColumInfo(columName);
			if (i > 0)
			{
				sb.append(parseItem.getSplitMark());
			}
			tmStr = getValue(columnInfo);
			sb.append(tmStr);
			i++;
		}
		return sb.toString();
	}
	
	public Object parseData(String value, String formatFunc) throws ParseException
	{		
		value = value.trim();
		if(formatFunc.length() == 0)
		{
			return value;
		}
		
		Object res = value;
		
		switch (formatFunc)
		{
		case "FORMAT_16W":
			res = Integer.valueOf(value,16).toString();
			break;
		case "FORMAT_IMSI_ENCRYPT":
			tmStr = value.length()>15?value.substring(0, 15):"0";
			res = Long.parseLong(tmStr, 16);
			break;
		case "FORMAT_LONG2DATE_yyyy-MM-dd HH:mm:ss":
			if(value.length()>13)
				value = value.substring(0,13);
			res = new Date(Long.parseLong(value));
		    break;
		case "FORMAT_INT2DATE_yyyy-MM-dd HH:mm:ss":
			res = new Date(Long.parseLong(value)*1000);
		    break;
		case "FORMAT_DATE_yyyy-MM-dd HH:mm:ss":
			res = dateFormat.parse(value);
		    break;
		case "FORMAT_DATE_yyyy-MM-dd HH:mm:ss.S":
			res = dateFormat2.parse(value);
			break;
		case "FORMAT_DATE_yyyy-MM-ddTHH:mm:ss.S":
			res = dateFormat2.parse(value.replace("T", " "));
			break;
		case "FORMAT_DATE_yyyyMMddHHmmssS":
			res = dateFormat3.parse(value);
			break;
		case "FORMAT_DATE_LONG":
			res = new Date(Long.parseLong(value));
			break;

		default:
			break;
		}
		
		return res;
		
	}
    
}
