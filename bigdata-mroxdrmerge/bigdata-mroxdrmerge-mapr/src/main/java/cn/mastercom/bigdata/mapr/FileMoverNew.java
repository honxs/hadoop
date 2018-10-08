package cn.mastercom.bigdata.mapr;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

public class FileMoverNew
{
	protected static Logger LOG = Logger.getLogger(FileMoverNew.class);

	/**
	 * 初始化的线程数
	 */
	public static final int MOVE_CONCURRENT_INIT = 5;

	/**
	 * 最大并发线程数
	 */
	public static final int MOVE_CONCURRENT_MAX = 30;

	/**
	 * 线程空闲存活时间
	 */
	public static final int MOVE_THREAD_ALIVE_SEC = 5;

	/**
	 * 阈值, 累计多少个文件后执行
	 */
	public static final int MOVE_ACTION_THRESHOLD = 10000;

	public static void main(String args[])
	{
		doMergestat(args[0], args[1]);
	}

	public static void doMergestat(String dataTime, String queueName)
	{
		Configuration conf = new Configuration();
		String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
		try
		{
			HDFSOper hdfsOper = new HDFSOper(conf);
			String day = dataTime.replace("01_", "");
			String dayHour = "";

			String xdrLocPath = mroXdrMergePath + "/xdr_loc/data_" + dataTime;
			String mroLocPath = mroXdrMergePath + "/mro_loc/data_" + dataTime;
			String filterWords = "";
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan))
			{
				filterWords = "Mins,MYLOG,output";
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
			{
				filterWords = "Mins,MYLOG,output,LIB";
			}

			String xdrsrcPath = "";
			String mrosrcPath = "";
			String suf = "";
			long start = System.currentTimeMillis();

			ThreadPoolExecutor executor = new ThreadPoolExecutor(MOVE_CONCURRENT_INIT, MOVE_CONCURRENT_MAX, MOVE_THREAD_ALIVE_SEC, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
			List<Future<Boolean>> moveResultFutures = new ArrayList<>();
			Map<String, AtomicInteger> counter = new HashMap<>();
			int failedCount = 0;
			int succeedCount = 0;

			for (int i = 0; i < 24; i++)
			{
				if (i < 10)
				{
					suf = "0" + i;
				}
				else
				{
					suf = "" + i;
				}
				xdrsrcPath = xdrLocPath + suf;
				mrosrcPath = mroLocPath + suf;
				dayHour = day + suf;
				// 移动xdr数据
				LOG.info("start moving " + xdrsrcPath);
				moveResultFutures.addAll(moveFile(hdfsOper, xdrsrcPath, filterWords, dayHour, day, suf, executor, counter));
				LOG.info("start moving " + mrosrcPath);
				moveResultFutures.addAll(moveFile(hdfsOper, mrosrcPath, filterWords, dayHour, day, suf, executor, counter));

				// 如果累计超过阈值个文件待移动，那么先执行
				if (moveResultFutures.size() >= MOVE_ACTION_THRESHOLD)
				{
					for (Future<Boolean> result : moveResultFutures)
					{
						if (!result.get())
						{
							failedCount++;
						}
						else
							succeedCount++;
					}
					LOG.info("Done. Size of taskList is " + moveResultFutures.size());
					moveResultFutures.clear();
				}
			}
			for (Future<Boolean> result : moveResultFutures)
			{
				if (!result.get())
					failedCount++;
				else
					succeedCount++;
			}
			// 删除全部成功的文件夹
			for (Map.Entry<String, AtomicInteger> entry : counter.entrySet())
			{
				if (entry.getValue().get() == 0)
				{
					hdfsOper.delete(entry.getKey());
				}
				else
				{
					LOG.error("Failed to move file in path '" + entry.getKey() + "'");
				}
			}
			LOG.info("Done. Size of taskList is " + moveResultFutures.size());
			LOG.info("Move file finished ！ Cost: " + (System.currentTimeMillis() - start) + "ms");
			LOG.info("Successfully-moved file num is " + succeedCount + ". Unsuccessfully-moved file num is " + failedCount + ".");
			moveResultFutures.clear();
			executor.shutdownNow();

			LOG.info("begin merge ！");

			MergestatGroup.doMergestatGroup(queueName, dataTime, mroXdrMergePath, conf, hdfsOper);
			LOG.info("merge finished !");
		}
		catch (Exception e)
		{
			LOG.info("mover error !", e);
		}
	}

	/**
	 * 
	 * @param hdfsOper
	 * @param srcFolder
	 *            原始文件夹
	 * @param filterWords
	 *            过滤
	 * @param
	 * @param suf
	 *            文件名添加后缀
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private static List<Future<Boolean>> moveFile(final HDFSOper hdfsOper, final String srcFolder, final String filterWords, final String dayhour, final String day, final String suf,
			final ThreadPoolExecutor executor, final Map<String, AtomicInteger> counter) throws FileNotFoundException, IllegalArgumentException, IOException
	{
		List<Future<Boolean>> result = new ArrayList<Future<Boolean>>();
		if (hdfsOper.checkDirExist(srcFolder))
		{
			List<FileStatus> fileList = hdfsOper.listFlolderStatus(srcFolder);
			for (FileStatus fs : fileList)
			{
				final String path = fs.getPath().toString();
				if (filter(path, filterWords))
				{
					List<FileStatus> tempList = hdfsOper.listFileStatus(path, true);
					for (final FileStatus tmpfs : tempList)
					{
						final String temppath = tmpfs.getPath().toString();
						if (filter(temppath, filterWords))
						{
							final Path dPath = new Path(temppath.replace(dayhour, day) + "_" + suf);
							if (!hdfsOper.checkDirExist(dPath.getParent().toString()))
							{
								hdfsOper.mkdir(dPath.getParent().toString());
							}
							// 计数器, 这里也可以设置为 path
							AtomicInteger count = counter.get(srcFolder);
							if (count == null)
							{
								count = new AtomicInteger(1);
								counter.put(srcFolder, count);
							}
							else
								count.getAndIncrement();

							result.add(executor.submit(new Callable<Boolean>()
							{

								@Override
								public Boolean call() throws Exception
								{
									boolean result = hdfsOper.getHdfs().rename(tmpfs.getPath(), dPath);
									if (result)
										counter.get(srcFolder).getAndDecrement();
									return result;
								}

							}));
						}
					}
					tempList.clear();
				}
			}
			fileList.clear();
		}
		return result;
	}

	private static boolean filter(String path, String filterWord)
	{
		String[] filters = filterWord.split(",", -1);
		for (String filter : filters)
		{
			if (path.contains(filter))
			{
				return false;
			}
		}
		return true;
	}

}
