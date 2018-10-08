package cn.mastercom.bigdata.util;

import org.apache.commons.logging.Log;
import org.apache.log4j.Logger;

import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class FilterSparkLogCallBack implements IWriteLogCallBack{

	protected static Logger log = Logger.getLogger(FilterSparkLogCallBack.class.getName());
	public String testname;
	public FilterSparkLogCallBack(){
		testname = "dl";
	}
	public void writeLog(LogType type, String strlog){
		log.info(strlog);
	};
}
