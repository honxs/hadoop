package cn.mastercom.bigdata.standalone.main;

import java.util.Collection;

public interface IWork 
{
	/**
	 * 业务名称
	 */
	String getName();
	
	/**
	 * 扫描任务的时间间隔
	 */
	int getInterval();
	
	/**
	 * 执行任务的线程数
	 */
	int getThreadCount();
	
	/**
	 * 业务初始化
	 */
	void init() throws Exception;
	
	/**
	 * 每过一段时间获取任务
	 */
	Collection<ATask> getTasks() throws Exception;
	
	/**
	 * 一批任务处理完成时调用
	 */
	void onTasksFinished() throws Exception;
}
