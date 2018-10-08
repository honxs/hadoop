package cn.mastercom.bigdata.standalone.main;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import cn.mastercom.bigdata.standalone.local.IOHelper;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class StartUp extends Thread
{
	protected static Logger LOG = Logger.getLogger(MainModel.class);

	private IWork m_Work;
	public boolean Enabled = true;

	private int m_TaskCount = 0;

	/**
	 * 获取当前任务数
	 */
	public int getTaskCount()
	{
		return m_TaskCount;
	}

	/**
	 * 获取当前已完成任务数
	 */
	private AtomicInteger m_curCnt = null;

	public AtomicInteger getAtomicInteger()
	{
		return m_curCnt;
	}

	/**
	 * 获取业务名称
	 */
	public String getWorkName()
	{
		return m_Work.getName();
	}

	public StartUp(IWork worker)
	{
		this.m_Work = worker;
	}

	/**
	 * runner
	 */
	public void run()
	{
		String name = m_Work.getName();
		LOG.info(name + " : thread start.");

		try
		{
			m_Work.init();

			LOG.info(name + " : init success.");

			long _lastDecodeTime = 0;
			long interval = 1000L * m_Work.getInterval();
			int threadCount = m_Work.getInterval();

			doSleep(2000);

			while (Enabled)
			{
				long now = new Date().getTime();
				if (now - _lastDecodeTime > interval)
				{
					_lastDecodeTime = now;
					work(threadCount);
				}

				doSleep(1000);
			}

		}
		catch (Exception e)
		{
			LOG.error(name + ":init error!\n" + IOHelper.getTraceMessage(e));
		}
	}

	private void doSleep(long _interval)
	{
		try
		{
			Thread.sleep(_interval);
		}
		catch (Exception e)
		{
		}
	}

	/**
	 * 获取任务
	 * 
	 * @param threadCount
	 */
	private void work(int threadCount)
	{
		Collection<ATask> tasks = null;

		try
		{
			tasks = m_Work.getTasks();

			if (tasks == null || tasks.size() == 0)
			{
				LOG.info("not find task.");
			}
			else
			{
				m_TaskCount = tasks.size();
				m_curCnt = new AtomicInteger(0);
				LOG.info("find " + m_TaskCount + " tasks.");
				work(threadCount, tasks);

				try
				{
					m_Work.onTasksFinished();
				}
				catch (Exception e)
				{
					LOG.error(getWorkName() + ":onTasksFinished error!" + IOHelper.getTraceMessage(e));
				}

				LOG.info("all tasks finished.");
			}
		}
		catch (Exception e)
		{
			LOG.warn(getWorkName() + ":getTasks error!\n" + IOHelper.getTraceMessage(e));
		}

	}

	/**
	 * 多线程执行任务
	 * 
	 * @param threadCount
	 * @param tasks
	 */
	@SuppressWarnings("rawtypes")
	private void work(int threadCount, Collection<ATask> tasks)
	{
		try
		{
			int tnum = Math.min(threadCount, tasks.size());
			// 创建一个线程池
			ExecutorService pool = Executors.newFixedThreadPool(tnum);
			// 创建多个有返回值的任务
			List<Future> listTask = new ArrayList<Future>();

			// 多线程解码
			for (ATask task : tasks)
			{
				// 执行任务并获取Future对象
				task.setStartUp(this);
				Future f1 = pool.submit(task);
				listTask.add(f1);
			}

			// 获取所有并发任务的运行结果
			for (Future f : listTask)
			{
				try
				{
					f.get();
				}
				catch (Exception e)
				{
					LOG.error(getWorkName() + ":ATask error!" + IOHelper.getTraceMessage(e));
				}
			}
			
			pool.shutdown();

		}
		catch (Exception e)
		{
			LOG.error(getWorkName() + ":StartUp error!" + IOHelper.getTraceMessage(e));
		}

	}

}
