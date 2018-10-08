package cn.mastercom.bigdata.util;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class Func
{
	public static final int YYS_YiDong = 1;
	public static final int YYS_LianTong = 2;
	public static final int YYS_DianXin = 3;
	public static final int YYS_UnKnown = 4;

	public static double addValue(double srcValue, double addValue)
	{
		if (srcValue == StaticConfig.Int_Abnormal)
		{
			return addValue;
		}

		if (addValue == StaticConfig.Int_Abnormal)
		{
			return srcValue;
		}

		return addValue + srcValue;
	}

	public static float getMax(float valueMax, float value)
	{
		if (valueMax == StaticConfig.Int_Abnormal || valueMax < value)
		{
			return value;
		}
		return valueMax;
	}

	public static float getMin(float valueMin, float value)
	{
		if (value == StaticConfig.Int_Abnormal)
			return valueMin;
		if (valueMin == StaticConfig.Int_Abnormal || valueMin > value)
		{
			return value;
		}
		return valueMin;
	}

	/**
	 * 确定运营商
	 * 
	 * @param freq
	 *            频点
	 * @return
	 */
	public static int getFreqType(int freq)
	{
		switch (freq)
		{
		case 1227:
		case 1252:
		case 1275:
		case 1277:
		case 1300:
		case 1302:
		case 1327:
		case 1350:
		case 1368:
		case 1396:
		case 1352:
		case 1400:
		case 1402:
		case 3683:
		case 3689:
		case 3738:
			return YYS_YiDong;
		
		case 400:
		case 401:
		case 450:
		case 500:
		case 1500:
		case 1506:
		case 3770:
		case 1600:
		case 1650:
		case 40340:
			return YYS_LianTong;

		case 75:
		case 100:
		case 1750:
		case 1775:
		case 1800:
		case 1825:
		case 1850:
		case 1870:
		case 2440:
		case 2446:
		case 2452:
		case 41140:
			return YYS_DianXin;

		default:
			if (freq >= 2410 && freq <= 2510)
			{// 800M
				return YYS_DianXin;
			}
			else if (freq >= 30000 && freq != 40340)
			{
				return YYS_YiDong;
			}
			return YYS_UnKnown;
		}
	}

	/**
	 * 根据loctp,确定 high mid low
	 * 
	 * @param loctp
	 * @return
	 */
	public static int getLocSource(String loctp)
	{
		if (loctp.contains("ll"))
		{
			return StaticConfig.LOCTYPE_HIGH;
		}
		else if (loctp.contains("wf") || loctp.contains("ru") || loctp.contains("wl"))// wl:家宽定位
		{
			return StaticConfig.LOCTYPE_MID;
		}
		else if (loctp.contains("fp") || loctp.contains("cl"))
		{
			return StaticConfig.LOCTYPE_LOW;
		}
		return 0;
	}

	/**
	 * 经度元整
	 * 
	 */
	public static long getRoundLongtitude(int size, long longtitude)
	{
		return longtitude / (100 * size) * (100 * size);
	}

	public static long getRoundLongtitude(int size, String longtitude)
	{
		long longtitudetemp = Long.parseLong(longtitude);
		return longtitudetemp / (100 * size) * (100 * size);
	}

	/**
	 * 右下经度
	 * 
	 */
	public static long getRoundBRLongtitude(int size, long longtitude)
	{
		return longtitude / (100 * size) * (100 * size) + (100 * size);
	}

	public static long getRoundBRLongtitude(int size, String longtitude)
	{
		long longtitudetemp = Long.parseLong(longtitude);
		return longtitudetemp / (100 * size) * (100 * size) + (100 * size);
	}

	/**
	 * 纬度元整
	 * 
	 */
	public static long getRoundLatitude(int size, long latitude)
	{
		return latitude / (90 * size) * (90 * size);
	}

	public static long getRoundLatitude(int size, String latitude)
	{
		long latitudetemp = Long.parseLong(latitude);
		return latitudetemp / (90 * size) * (90 * size);
	}

	/**
	 * 左上纬度
	 * 
	 */
	public static long getRoundTLLatitude(int size, long latitude)
	{
		return latitude / (90 * size) * (90 * size) + (90 * size);
	}

	public static long getRoundTLLatitude(int size, String latitude)
	{
		long latitudetemp = Long.parseLong(latitude);
		return latitudetemp / (90 * size) * (90 * size) + (90 * size);
	}

	public static int getSampleConfidentType(String loctp, int samState, int testType)
	{
		if (samState == StaticConfig.ACTTYPE_IN && loctp.contains("ll"))
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.IH;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.IL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_IN && (loctp.contains("wf") || loctp.contains("ru") || loctp.contains("hb")))
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.IM;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.IL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_IN && (loctp.contains("fp") || loctp.contains("cl")))
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.IL;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.IL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_OUT && loctp.contains("ll"))
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OH;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.OM;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.OM;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_OUT && (loctp.contains("wf") || loctp.contains("ru") || loctp.contains("hb")))
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OM;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.OL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_OUT && (loctp.contains("fp") || loctp.contains("cl")))
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.OL;
			}
		}
		return 0;

	}

	/**
	 * 常驻用户加密
	 * @param value
	 * @return
	 */
	public static long getEncryptResident(long value)
	{
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.ResidentEncrypt))
		{
			return StringUtil.EncryptStringToLongByXOR(value+"");
		}
		return value;
	}
	
	public static String getEncryptResident(String value)
	{
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.ResidentEncrypt))
		{
			return StringUtil.EncryptStringToLongByXOR(value)+"";
		}
		return value;
	}

	/**
	 * 所有加密
	 * @param value
	 * @return
	 */
	public static String getEncrypt(String value)
	{
		try {
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.Encrypt)) {
				return StringUtil.EncryptStringToLongByXOR(value) + "";
			}
		} catch (NullPointerException ignored) {}
		return value;
	}
	
	public static long getEncrypt(long value)
	{
		try {
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.Encrypt)) {
				return StringUtil.EncryptStringToLongByXOR(value + "");
			}
		} catch (NullPointerException ignored) {}
		return value;
	}

	/**
	 * 解密
	 * @param value
	 * @return
	 */
	public static String getDecrypt(String value)
	{
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Encrypt))
		{
			return StringUtil.DecryptStringToLongByXOR(value)+"";
		}
		return value;
	}

	public static long getDecrypt(long value)
	{
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Encrypt))
		{
			return StringUtil.DecryptStringToLongByXOR(value+"");
		}
		return value;
	}

	public static int getSampleConfidentType(int locSource, int samState, int testType)
	{
		if (samState == StaticConfig.ACTTYPE_IN && locSource == StaticConfig.LOCTYPE_HIGH)
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.IH;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.IL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_IN && locSource == StaticConfig.LOCTYPE_MID)
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.IM;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.IL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_IN && locSource == StaticConfig.LOCTYPE_LOW)
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.IL;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.IL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_OUT && locSource == StaticConfig.LOCTYPE_HIGH)
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OH;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.OM;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.OM;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_OUT && locSource == StaticConfig.LOCTYPE_MID)
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OM;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.OL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_OUT && locSource == StaticConfig.LOCTYPE_LOW)
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.OL;
			}
		}
		else if (testType != StaticConfig.TestType_HiRail)// 高铁有位置
		{
			return StaticConfig.NLoc;
		}
		return 0;
	}

	/**
	 * 根据location ，确定 high mid low
	 * 
	 * @param location
	 * @return
	 */
	public static int getLocSource(int location)
	{
		if (location == 2 || location == 10)// uri定位
		{
			return StaticConfig.LOCTYPE_MID;
		}
		return 0;
	}
	
	public static int getDirection(int direc1, int direc2)
	{
		if ((direc1 == StaticConfig.SOUTH && direc2 == StaticConfig.NORTH) || (direc1 == StaticConfig.NORTH && direc2 == StaticConfig.SOUTH)
			|| (direc1 == StaticConfig.EAST && direc2 == StaticConfig.WEST) || (direc1 == StaticConfig.WEST && direc2 == StaticConfig.EAST))
		{
			return StaticConfig.MIDDLE;
		}
		return direc1;
	}

	public static int getDateType(int time)
	{
		int hour = FormatTime.getHour(time);
		if ((hour >= 10 && hour <= 11) || (hour >= 15 && hour <= 16))
		{
			return StaticConfig.WORKTIME;
		}
		else if ((hour >= 0 && hour <= 5) || (hour >= 22 && hour <= 23))
		{
			return StaticConfig.HOMETIME;
		}
		return StaticConfig.OTHERTIME;
	}
}
