package cn.mastercom.bigdata.util;

public class BackGroundWorker
{
	private IBackGroundWorkerDo rc;// 请求主体
	private Object[] args;

	public BackGroundWorker(IBackGroundWorkerDo rc)
	{
		this.rc = rc;
	}

	public void start(Object[] objects)
	{ 
		this.args = objects;
		
		// 开始请求
		final Thread t = new Thread(new Runnable()
		{
			public void run()
			{
				rc.onStart();
				
				try
				{
					rc.doSomeThing(args);// 响应请求			
				}
				catch (Exception e)
				{
					rc.writeLog("error: " + e.getMessage());
					rc.onFailure(); // 如果执行失败
				}
				rc.onSuccess();// 如果执行成功
				
				rc.onEnd();
			}
		});
		t.start();
	}

}


