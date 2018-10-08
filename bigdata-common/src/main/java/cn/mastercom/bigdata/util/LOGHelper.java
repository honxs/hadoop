package cn.mastercom.bigdata.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class LOGHelper
{
	public static final int LogLevel_ALL = 100;
	public static final int LogLevel_DEBUG = 40;
	public static final int LogLevel_INFO = 30;
	public static final int LogLevel_WARN = 20;
	public static final int LogLevel_ERROR = 10;
	public static final int LogLevel_NONE = 0;
	/**
	 * key为try catch的类型，value为次数
	 */
	public static HashMap<String,Long> mLogMap = new HashMap<>();
	
	
	private List<IWriteLogCallBack> logList = new ArrayList<IWriteLogCallBack>();
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(); 
	private PrintStream printStream = new PrintStream(byteArrayOutputStream);
	private int logLevel = LogLevel_ALL;
	private static boolean isWindows = false;
	private LOGHelper()
	{
        String os = System.getProperty("os.name");
        //TODO 应该还加个mac的标识，因为有的人是在mac上进行开发的
        if(os.toLowerCase().startsWith("win")){
            isWindows = true;
        }
	}
	
	private static LOGHelper instance;
	public static LOGHelper GetLogger()
	{
		if(instance == null)
		{
			instance = new LOGHelper();
		}
		return instance;
	}
	
	public void addWriteLogCallBack(IWriteLogCallBack writeLogCallBack)
	{
		logList.add(writeLogCallBack);
	}
	
	public void removeWriteLogCallBack(IWriteLogCallBack writeLogCallBack)
	{
		logList.remove(writeLogCallBack);
	}

	/**
	 * 最终打印各种异常信息的条数
	 */
	public void finalWriteLogCount(){
		for (String key : mLogMap.keySet()) {
			String errorCountValue = key+" 条数: "+mLogMap.get(key);
			if(isWindows){
				System.out.println(key+" 条数: "+mLogMap.get(key));
			}
			writeLog(LogType.error,errorCountValue);
		}
		mLogMap.clear();
	}

	/**
	 *
	 * @param logType LOG 级别
	 * @param logLocation LOG标识，最后会输出此log标识catch了多少条。尽量不同的catch写的logLocation要不同
	 * @param log log内容
	 * @param e 报错方法栈
	 */
	public void writeLog(LogType logType,String logLocation, String log, Exception e)
	{
	    if(isWindows){
	        e.printStackTrace();
        }
        //每种报错有多少条收集起来，只收集100条,超过100条只收集条数
		if (ifOverNumAndCollectCount(logLocation)){
			return;
		}
		byteArrayOutputStream.reset();
		e.printStackTrace(printStream);  	
		String strlog = logLocation+"\t"+ log + "\n" + "MESSAGE：" +
				e.getMessage() + "\n" + byteArrayOutputStream.toString();
		writeLog(logType, strlog);
	}



	// 这个和前面的writeLog是一样的
	public void writeDetailLog(LogType logType,String logLocation, String log, Exception e)
	{
		if(isWindows){
			e.printStackTrace();
		}
		//每种报错有多少条收集起来，只收集100条,超过100条只收集条数
		if (ifOverNumAndCollectCount(logLocation)){
			return;
		}
		StringBuffer sb = new StringBuffer();
		sb.delete(0, sb.length());
		sb.append(logLocation).append("\t");
		sb.append(log + "\n" + "MESSAGE：" + "\n");
		StackTraceElement[] trace = e.getStackTrace();
		for (StackTraceElement stackTraceElement : trace)
		{
			sb.append("\tat " + stackTraceElement + "\n");
		}
		writeLog(logType, sb.toString());
	}
	
	public void writeLog(LogType logType, String strlog)
	{
        if(isWindows){
            System.err.println(strlog);
        }
		if(logType == LogType.info)
		{
			if(logLevel >= LogLevel_INFO)
			{
				for(IWriteLogCallBack item : logList)
				{
					item.writeLog(logType, strlog);
				}
			}
		}
		else if(logType == LogType.debug)
		{
			if(logLevel >= LogLevel_DEBUG)
			{
				for(IWriteLogCallBack item : logList)
				{
					item.writeLog(logType, strlog);
				}
			}
		}
		else if(logType == LogType.warn)
		{
			if(logLevel >= LogLevel_WARN)
			{
				for(IWriteLogCallBack item : logList)
				{
					item.writeLog(logType, strlog);
				}
			}
		}
		else if(logType == LogType.error)
		{
			if(logLevel >= LogLevel_ERROR)
			{
				for(IWriteLogCallBack item : logList)
				{
					item.writeLog(logType, strlog);
				}	
			}			
		}
		
		return;	
	}
	
	public static String getFormatLog(LogType logType, String log)
	{
		if(logType == LogType.info)
		{
			return getFormatLogInfo(log);
		}
		else if(logType == LogType.debug)
		{
			return getFormatLogDebug(log);
		}
		else if(logType == LogType.warn)
		{
			return getFormatLogWarn(log);
		}
		else if(logType == LogType.error)
		{
			return getFormatLogError(log);
		}
        return log;
	}

	private boolean ifOverNumAndCollectCount(String logLocation) {
		if (mLogMap.containsKey(logLocation)) {
			Long count = mLogMap.get(logLocation)+1;
			mLogMap.put(logLocation,count);
			if(count>1000){
				return true;
			}
		}else {
			mLogMap.put(logLocation,1L);
		}
		return false;
	}

	public int getLogLevel()
	{
		return logLevel;
	}
	
	public void setLogLevel(int logLevel)
	{
		this.logLevel = logLevel;
	}

    public static String getFormatLogInfo(String log)
    {
    	return "[" + format.format(new Date()) + "][INFO] " + log;
    }
    
    public static String getFormatLogWarn(String log)
    {
    	return "[" + format.format(new Date()) + "][WARN] " + log;
    }
    
    public static String getFormatLogError(String log)
    {
    	return "[" + format.format(new Date()) + "][ERROR] " + log;
    }
    
    public static String getFormatLogDebug(String log)
    {
    	return "[" + format.format(new Date()) + "][DEBUG] " + log;
    }
	
}
