package cn.mastercom.bigdata.util;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import cn.mastercom.bigdata.util.DataAdapterConf.ColumnInfo;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;

public class DataAdapterReader implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ParseItem parseItem;
	public String[] tmStrs;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
	private SimpleDateFormat dateFormat3 = new SimpleDateFormat("yyyyMMddHHmmssS");
	private StringBuffer sb = new StringBuffer();
	
	public DataAdapterReader(ParseItem parseItem)
	{
		this.parseItem = parseItem;
	}

	public ParseItem getParseItem() {
		return parseItem;
	}

	public StringBuffer getTmStrs()
	{
		String fenge = parseItem.getSplitMark();
		if (fenge.contains("\\"))
		{
			fenge = fenge.replace("\\", "");
		}
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < tmStrs.length; i++)
		{
			if (i != 0)
			{
				sb.append(fenge);
			}
			sb.append(tmStrs[i].trim());
		}
		return sb;
	}

	public boolean readData(String strData, int maxSplitCount)
	{
		tmStrs = strData.split(parseItem.getSplitMark(), maxSplitCount);
		return true;
	}

	public boolean readData(String[] strData)
	{
		tmStrs = strData;
		return true;
	}

	public boolean readData(String strData)
	{
		return readData(strData, parseItem.getSplitSize());
	}

	public int GetDataLenth()
	{
		return tmStrs.length;
	}

	public ColumnInfo getColumnInfo(String columName)
	{
		ColumnInfo columnInfo = parseItem.getColumInfo(columName);
		return columnInfo;
	}

	public int GetIntValue(String columName, int defaultValue) throws ParseException
	{
		ColumnInfo columnInfo = parseItem.getColumInfo(columName);
		if (columnInfo == null || columnInfo.pos >= tmStrs.length || tmStrs[columnInfo.pos].length() == 0
				|| tmStrs[columnInfo.pos].equals("NIL") || tmStrs[columnInfo.pos].equals("null")
				|| tmStrs[columnInfo.pos].equals("NULL") || tmStrs[columnInfo.pos].equals("--")
				|| tmStrs[columnInfo.pos].equals("\\N"))
		{
			return defaultValue;
		}
		Object o = parseData(tmStrs[columnInfo.pos], columnInfo.formatFunc);
		if (o instanceof Integer)
		{
			return (int) o;
		}
		return Integer.parseInt(o.toString());
	}

	public long GetLongValue(String columName, long defaultValue) throws ParseException
	{
		ColumnInfo columnInfo = parseItem.getColumInfo(columName);
		if (columnInfo == null || columnInfo.pos >= tmStrs.length || tmStrs[columnInfo.pos].length() == 0
				|| tmStrs[columnInfo.pos].equals("NIL") || tmStrs[columnInfo.pos].equals("null")
				|| tmStrs[columnInfo.pos].equals("NULL") || tmStrs[columnInfo.pos].equals("--")
				|| tmStrs[columnInfo.pos].equals("\\N"))
		{
			return defaultValue;
		}

		return Long.parseLong(parseData(tmStrs[columnInfo.pos], columnInfo.formatFunc).toString());
	}

	public String GetStrValue(String columName, String defaultValue) throws ParseException
	{
		ColumnInfo columnInfo = parseItem.getColumInfo(columName);
		if (columnInfo == null || columnInfo.pos >= tmStrs.length || tmStrs[columnInfo.pos].length() == 0
				|| tmStrs[columnInfo.pos].equals("NIL") || tmStrs[columnInfo.pos].equals("null")
				|| tmStrs[columnInfo.pos].equals("NULL") || tmStrs[columnInfo.pos].equals("--")
				|| tmStrs[columnInfo.pos].equals("\\N"))
		{
			return defaultValue;
		}
		return parseData(tmStrs[columnInfo.pos], columnInfo.formatFunc).toString().trim();
	}

	public Double GetDoubleValue(String columName, double defaultValue) throws ParseException
	{
		ColumnInfo columnInfo = parseItem.getColumInfo(columName);
		if (columnInfo == null || columnInfo.pos >= tmStrs.length || tmStrs[columnInfo.pos].length() == 0
				|| tmStrs[columnInfo.pos].equals("NIL") || tmStrs[columnInfo.pos].equals("null")
				|| tmStrs[columnInfo.pos].equals("NULL") || tmStrs[columnInfo.pos].equals("--")
				|| tmStrs[columnInfo.pos].equals("\\N"))
		{
			return defaultValue;
		}

		Object o = parseData(tmStrs[columnInfo.pos], columnInfo.formatFunc);
		if (o instanceof Double)
		{
			return (double) o;
		}
		return Double.parseDouble(o.toString());
	}

	/**
	 * 将传过来的字段列拼接成字符串
	 * 
	 * @param coluName
	 *            字段
	 * @return
	 */
	public String getAppendString(List<String> columnNameList)
	{
		sb.delete(0, sb.length());
		String splitMark = parseItem.getSplitMark().replaceAll("\\\\", "");
		int i = 0; 
		for (String columnName : columnNameList)
		{
			ColumnInfo columnInfo = parseItem.getColumInfo(columnName);
			if (i == 0) {
				sb.append(tmStrs[columnInfo.pos]);
			}else{
				sb.append(splitMark);
				sb.append(tmStrs[columnInfo.pos]);
			}
			i++;
		}
		return sb.toString();
	}

	public Date GetDateValue(String columName) throws ParseException
	{
		Date date = dateFormat.parse("1970-01-01 00:00:00");
		
		return GetDateValue(columName, date);
	}
	
	public Date GetDateValue(String columName, Date defaultValue) throws ParseException
	{
		ColumnInfo columnInfo = parseItem.getColumInfo(columName);
		if (columnInfo == null || columnInfo.pos >= tmStrs.length || tmStrs[columnInfo.pos].length() == 0
				|| tmStrs[columnInfo.pos].equals("NIL") || tmStrs[columnInfo.pos].equals("null")
				|| tmStrs[columnInfo.pos].equals("NULL") || tmStrs[columnInfo.pos].equals("--")
				|| tmStrs[columnInfo.pos].equals("\\N"))
		{
			return defaultValue;
		}
		return (Date) (parseData(tmStrs[columnInfo.pos], columnInfo.formatFunc));
	}

	public String getStrValue(int pos, String defaultValue)
	{
		if (pos >= tmStrs.length)
		{
			return defaultValue;
		}
		return tmStrs[pos];
	}

	public static void main(String[] args) throws Exception
	{
		DataAdapterReader dar = new DataAdapterReader(null);
		Object obj = dar.parseData("FE6947791D526862F303802BEAE81139", "FORMAT_IMSI_ENCRYPT");
		System.out.println(obj.toString());
	}

	private String tmStr;

	public Object parseData(String value, String formatFunc) throws ParseException
	{
		value = value.trim();
		if (formatFunc.length() == 0)
		{
			return value;
		}

		Object res = value;

		switch (formatFunc)
		{
			case "FORMAT_STRING":
				res = StringUtil.EncryptStringToLong(value);
				break;
			case "FORMAT_16W":
				res = Long.valueOf(value, 16).toString();
				break;
			case "FORMAT_IMSI_ENCRYPT":
				tmStr = value.length() > 15 ? value.substring(0, 15) : "0";
				res = Long.parseLong(tmStr, 16);
				break;
			case "FORMAT_LONG2DATE_yyyy-MM-dd HH:mm:ss":
				if (value.length() > 13) value = value.substring(0, 13);
				res = new Date(Long.parseLong(value));
				break;
			case "FORMAT_INT2DATE_yyyy-MM-dd HH:mm:ss":
				res = new Date(Long.parseLong(value) * 1000);
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
			case "FORMAT_DATE_LONG":
				res = new Date(Long.parseLong(value));
				break;
			case "FORMAT_DATE_yyyyMMddHHmmssS":
				res = dateFormat3.parse(value);
				break;

			case "FORMAT_INT_FROM_LotLat":
			{
				Double d = Double.parseDouble(value) * 10000000;
				res = d.intValue();
				break;
			}

			case "FORMAT_LotLat_FROM_INT":
			{
				res = Integer.parseInt(value) / 10000000D;
				break;
			}

			case "FORMAT_INT_FROM_DOUBLE":
			{
				Double d = Double.parseDouble(value);
				res = d.intValue();
				break;
			}

			default:
				break;
		}

		return res;

	}

}
