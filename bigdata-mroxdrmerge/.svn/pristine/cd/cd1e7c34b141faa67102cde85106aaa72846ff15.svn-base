package cn.mastercom.bigdata.pack;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PackMain 
{
     
	public static void main(String[] args) throws Exception 
	{ 	
		String strDate = args[0];
		String queneName = args[1];
		
		System.out.println("xdr data path is null, check the inputs " + strDate + " " + queneName);
		
		///flume/xdr/%s
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Date statTime = format.parse(strDate);
		
		
		System.out.println("初始化配置...");
		//MainModel.GetInstance().loadConfig();
		//MainModel.GetInstance().getAppConfig().saveConfigure();
		System.out.println("初始化配置完毕！");
		
		
		
		System.out.println("正在进行xdr和location二表关联...");
		XdrLocationMerge xdrLocationMerge = new XdrLocationMerge(statTime, queneName);
		xdrLocationMerge.run();
		System.out.println("xdr和location二表关联完毕！");
		
		
		
		System.out.println("正在进行xdr和mro关联...");
		//xdrMroMerge(statTime);
		XdrMroMerge xdrMroMerge = new XdrMroMerge(statTime, queneName);
		xdrMroMerge.run();
		System.out.println("xdr和mro关联完毕！");
		
	}
	
	
}
