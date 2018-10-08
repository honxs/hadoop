package cn.mastercom.bigdata.util;

public interface IWriteLogCallBack
{
	public enum LogType {info, debug, error, warn; }	
    public void writeLog(LogType type, String strlog);
	
}
