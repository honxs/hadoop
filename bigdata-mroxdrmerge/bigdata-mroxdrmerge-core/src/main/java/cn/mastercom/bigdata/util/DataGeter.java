package cn.mastercom.bigdata.util;

import cn.mastercom.bigdata.StructData.StaticConfig;

public class DataGeter
{
	
   //long
   public static long GetLong(String strData, long defaultValue)
   {
	   strData = strData.trim();
	   if(strData == null || strData.length() == 0 || strData.equals("NULL") || strData.equals("\\N"))
	   {
		   return defaultValue;
	   }	  
	   return Long.parseLong(strData);  
   }
	
   public static long GetLong(String strData)
   {
	   return GetLong(strData, StaticConfig.Long_Abnormal);
   }
   
   //int
   public static int GetInt(String strData, int defaultValue)
   {
	   strData=strData.trim();
	   if(strData == null || strData.length() == 0 || strData.toUpperCase().equals("NULL") || strData.equals("\\N"))
	   {
		   return defaultValue;
	   }
	   
	   return Integer.parseInt(strData);  
   }
	
   public static int GetInt(String strData)
   {
	   return GetInt(strData, StaticConfig.Int_Abnormal);
   }
   
   //short
   public static short GetShort(String strData, short defaultValue)
   {
	   strData=strData.trim();
	   if(strData == null || strData.length() == 0 || strData.toUpperCase().equals("NULL") || strData.equals("\\N"))
	   {
		   return defaultValue;
	   }
	   
	   return Short.parseShort(strData);  
   }
	
   public static short GetShort(String strData)
   {
	   return GetShort(strData, StaticConfig.Short_Abnormal);
   }
   
   //tiny int
   public static short GetTinyInt(String strData, short defaultValue)
   {
	   strData=strData.trim();
	   if(strData == null || strData.length() == 0 || strData.toUpperCase().equals("NULL") || strData.equals("\\N"))
	   {
		   return defaultValue;
	   }
	   
	   return Short.parseShort(strData);  
   }
	
   public static short GetTinyInt(String strData)
   {
	   return GetTinyInt(strData, StaticConfig.TinyInt_Abnormal);
   }
   
   //double
   public static double GetDouble(String strData, double defaultValue)
   {
	   strData=strData.trim();
	   if(strData == null || strData.length() == 0 || strData.toUpperCase().equals("NULL") || strData.equals("\\N"))
	   {
		   return defaultValue;
	   }
	   
	   return Double.parseDouble(strData);  
   }
	
   public static double GetDouble(String strData)
   {
	   return GetDouble(strData, StaticConfig.Double_Abnormal);
   }
   
   //float
   public static float GetFloat(String strData, float defaultValue)
   {
	   strData=strData.trim();
	   if(strData == null || strData.length() == 0 || strData.toUpperCase().equals("NULL") || strData.equals("\\N"))
	   {
		   return defaultValue;
	   }
	   
	   return Float.parseFloat(strData);  
   }
	
   public static float GetFloat(String strData)
   {
	   return GetFloat(strData, StaticConfig.Float_Abnormal);
   }
   
   //String
   public static String GetString(String strData, String defaultValue)
   {
	   if(strData == null || strData.length() == 0)
	   {
		   return defaultValue;
	   }
	   
	   return strData;  
   }
	
   public static String GetString(String strData)
   {
	   return GetString(strData, StaticConfig.String_Abnormal);
   }
  
	
}
