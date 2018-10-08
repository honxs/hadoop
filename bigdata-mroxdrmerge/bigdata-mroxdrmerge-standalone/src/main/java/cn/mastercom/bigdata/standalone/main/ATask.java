package cn.mastercom.bigdata.standalone.main;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import cn.mastercom.bigdata.standalone.local.IOHelper;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public abstract class ATask implements Callable<Object>
{
	public static Logger LOG = Logger.getLogger(MainModel.class);

	private static final byte[] lock = new byte[0]; // 特殊的instance变量
	private StartUp m_StartUp;

	public final void setStartUp(StartUp startUp)
	{
		m_StartUp = startUp;
	}

	@Override
	public final Object call() throws Exception
	{
		try
		{
			List<Exception> es = run();

			if (es != null && es.size() > 0)
			{
				for (Exception e : es)
				{
					LOG.warn("Work:" + m_StartUp.getWorkName() + "\n" + IOHelper.getTraceMessage(e));
				}
			}
			return true;
		}
		catch (Exception e)
		{
			String info = "Work:" + m_StartUp.getWorkName() + "\n";
			LOG.error(info + IOHelper.getTraceMessage(e));
			return false;
		}
		finally
		{
			try
			{
				runFinally();
			}
			catch (Exception e)
			{
				String info = "Work:" + m_StartUp.getWorkName() + "\n";
				LOG.error(info + IOHelper.getTraceMessage(e));
			}
			synchronized (lock)
			{
				int cnt = m_StartUp.getAtomicInteger().incrementAndGet();
				LOG.info(String.valueOf(cnt) + "/" + String.valueOf(m_StartUp.getTaskCount()) + " tasks finished...");
			}
		}
	}

	/**
	 * 跑任务
	 */
	public abstract List<Exception> run();

	/**
	 * finally
	 */
	public abstract void runFinally() throws Exception;

}
