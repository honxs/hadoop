package cn.mastercom.bigdata.util;

public interface IBackGroundWorkerDo
{
	// 执行成功的动作。用户可以覆盖此方法
	public void onSuccess();

	// 执行失败的动作。用户可以覆盖此方法
	public void onFailure();
	
	// 执行开始的动作。用户可以覆盖此方法
	public void onStart();
	
	// 执行结束的动作。用户可以覆盖此方法
	public void onEnd();
	
	public void writeLog(String log);

	// 用户必须实现这个抽象方法，告诉子线程要做什么
	public void doSomeThing(Object[] args); 
	
}
