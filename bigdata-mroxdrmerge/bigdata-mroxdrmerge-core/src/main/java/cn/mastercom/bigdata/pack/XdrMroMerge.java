package cn.mastercom.bigdata.pack;
import java.text.SimpleDateFormat;
import java.util.Date;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.SSHHelper;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class XdrMroMerge implements IWriteLogCallBack
{
	private Date statDate = null;
	private String queneName = "";
   
	public XdrMroMerge(Date statDate, String queneName)
	{
	   this.statDate = statDate;	
	   this.queneName = queneName;
	}
	
	public boolean run()
	{
		try
		{		
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
			String cmd = String.format("hadoop jar %s/MroXdrMerge/MroXdrMerge_allstat.jar %s %s %s %s", 
					MainModel.GetInstance().getExePath(), 
					queneName, 
					"01_" + format.format(statDate).substring(2, 8), 
					MainModel.GetInstance().getAppConfig().getXdrDataPath() + "/" + format.format(statDate),
					"XDRPREPARE,XDRLOC,MROFORMAT,MROLOC,MERGESTAT");
			
	        //执行脚本
			SSHHelper sshHelper = null;
//			SSHHelper sshHelper = new SSHHelper(MainModel.GetInstance().getAppConfig().getSSHHost(),
//					MainModel.GetInstance().getAppConfig().getSSHPort(),
//					MainModel.GetInstance().getAppConfig().getSSHUser(),
//					MainModel.GetInstance().getAppConfig().getSSHPwd(), 
//					this);
			
			System.out.println("开始执行Mro&Xdr关联...");
			sshHelper.excuteCmd(cmd, 2);
			System.out.println("执行Mro&Xdr关联！");
			
		}
		catch (Exception e)
		{
			writeLog(LogType.error, e.getMessage());
			return false;
		}
		return true;
	}

	@Override
	public void writeLog(LogType type, String strlog)
	{
		System.out.println(strlog);
	}
	
	
	
	
	
}
