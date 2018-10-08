package cn.mastercom.bigdata.util;

import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

import com.jcraft.jsch.JSchException;

public class SSHBgWorker
{
	private SSHHelper ssh;
	private BackGroundWorker bgWorker;
	private IWriteLogCallBack writeLog;
	private boolean isWorkFinish;
	
	public SSHBgWorker(String host, int port, String user, String pwd, IWriteLogCallBack writeLog) throws JSchException
	{
		ssh = new SSHHelper(host, port, user, pwd, writeLog);
		this.writeLog = writeLog;
		isWorkFinish = true;
		bgWorker = new BackGroundWorker(new BgWorkerContent(ssh, writeLog));
	}
	
	public boolean getIsWorkFinish()
	{
		return isWorkFinish;
	}
	
	public boolean executeCmd(String cmd)
	{
		Object[] oo = new Object[1];
		oo[0] = cmd;
		
		try
		{
			bgWorker.start(oo);
		}
		catch (Exception e)
		{
			writeLog.writeLog(LogType.error, e.getMessage());
			return false;
		}

		return true;
	}
	
	private void setIsWorkFinish(boolean isWorkFinish)
	{
		this.isWorkFinish = isWorkFinish;
	}
	
	private class BgWorkerContent implements IBackGroundWorkerDo
	{
		private IWriteLogCallBack writeLog;
		private SSHHelper ssh;
		
		public BgWorkerContent(SSHHelper ssh, IWriteLogCallBack writeLog)
		{
			super();
			
			this.writeLog = writeLog;
			this.ssh = ssh;
		}
		
		@Override
		public void writeLog(String strLog)
		{
			writeLog.writeLog(LogType.error, strLog);
		}
		
		@Override
		public void onStart()
		{
			setIsWorkFinish(false);
		}
		
		@Override
		public void onEnd()
		{ 
			setIsWorkFinish(true);
		}

		@Override
		public void doSomeThing(Object[] args)
		{
			writeLog.writeLog(LogType.info, "开始执行命令...");
			writeLog.writeLog(LogType.info, "命令：" + (String)args[0]);
			
			String cmd = (String)args[0];
			ssh.excuteCmd(cmd, 1);
			
			writeLog.writeLog(LogType.info, "执行命令结束！");
		}

		@Override
		public void onSuccess()
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onFailure()
		{
			// TODO Auto-generated method stub
			
		}
		
		
	}
	
}
