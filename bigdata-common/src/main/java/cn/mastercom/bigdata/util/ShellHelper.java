package cn.mastercom.bigdata.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ShellHelper
{	
	public static void EXEC_CMD(String cmd)
	{
		EXEC_CMD(cmd, null);
	}

	public static void EXEC_CMD(String cmd, IWriteLogCallBack writeLog)
	{
		try
		{
			Process process = null;
			process = Runtime.getRuntime().exec(cmd);
			
			if(writeLog != null)
			{
				BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
				String line = "";
				while ((line = input.readLine()) != null)
				{
					writeLog.writeLog(IWriteLogCallBack.LogType.info, line);
				}
				input.close();
			}

		}
		catch (IOException e)
		{
			if(writeLog != null)
			{
				writeLog.writeLog(IWriteLogCallBack.LogType.error, "excute command error! " + e.getMessage());
			}
			
		}

	}

	

}
