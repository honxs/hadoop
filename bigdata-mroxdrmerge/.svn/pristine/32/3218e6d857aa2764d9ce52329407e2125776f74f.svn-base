package cn.mastercom.bigdata.util;

import java.io.*;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FileStore
{
	String File = null;
	int FieldCount = 0;

	List<String> Datas = new ArrayList<String>();

	StringBuilder SB = new StringBuilder();
	int _FieldCount = 0;

	public int MaxSize = 100000;

	private byte[] lock = new byte[0]; // 特殊的instance变量

	/*
	 * 默认按行处理
	 */
	public FileStore(String file)
	{
		File = file;
		FieldCount = 1;
	}

	/*
	 * 按行处理
	 */
	public void AddLine(String line)
	{
		synchronized (lock)
		{
			Datas.add(line);
			if (Datas.size() >= MaxSize)
			{
				Flush();
			}
		}
	}

	/*
	 * 按行处理
	 */
	public void AddLine(Collection<String> lines)
	{
		synchronized (lock)
		{
			Datas.addAll(lines);
			if (Datas.size() >= MaxSize)
			{
				Flush();
			}
		}
	}

	/*
	 * 按字段处理
	 */
	public FileStore(String file, int fieldCount)
	{
		File = file;
		FieldCount = fieldCount;
	}

	/*
	 * 按字段处理
	 */
	private void append(boolean end, Object[] objects)
	{
		if (objects != null)
		{
			for (Object object : objects)
			{
				_FieldCount++;

				SB.append("\t");
				if (object == null)
				{
					SB.append("");
				}
				else
				{
					SB.append(String.valueOf(object));
				}
			}
		}

		if (end)
		{
			if (_FieldCount != FieldCount)
			{
				System.out.println(">>>FileStore: 参数个数不对,已忽略该记录:预设=" + FieldCount + ",实际=" + _FieldCount);
			}
			else
			{
				Datas.add(SB.substring(1));
			}

			SB.setLength(0);
			_FieldCount = 0;

			if (Datas.size() >= MaxSize)
			{
				Flush();
			}
		}
	}

	/*
	 * 按字段处理 不可多线�?
	 */
	public void Append(boolean end, Object... objects)
	{
		append(end, objects);
	}

	/*
	 * 按字段处理 线程安全�?
	 */
	public void AddData(Object... objects)
	{
		synchronized (lock)
		{
			append(true, objects);
		}
	}

	public void Flush()
	{
		synchronized (lock)
		{
			Export(File, Datas);
			Datas.clear();
		}
	}

	public static void Export(String path, String table, String fileName, List<String> datas)
	{
		try
		{
			String strDir = path + "/" + table;
			File dir = new File(strDir);
			if (!dir.exists())
			{
				dir.mkdir();
			}
			Export(strDir + "/" + fileName, datas);
		}
		catch (Exception e)
		{
			System.out.println(IOHelper.getTraceMessage(e));
		}
	}

	/*
	 * charsetName = utf-8 append = true
	 */
	public static void Export(String file, List<String> datas)
	{
		Export(file, datas, true);
	}

	/*
	 * charsetName = utf-8
	 */
	public static void Export(String file, List<String> datas, boolean append)
	{
		Export(file, datas, append, "utf-8");
	}

	public static String NEW_LINE = System.getProperty("line.separator");

	/*
	 * 输出到文�?
	 */
	public static void Export(String file, List<String> datas, boolean append, String charsetName)
	{
		FileOutputStream fos = null;
		FileLock fileLock = null;
		OutputStreamWriter osw = null;
		BufferedWriter bw = null;
		try
		{
			IOHelper.MakeParentDir(file);
			fos = new FileOutputStream(file, append);

			// 对该文件加锁
			do
			{
				try
				{
					fileLock = fos.getChannel().tryLock();
				}
				catch (Exception e)
				{
				}
			} while (fileLock == null);

			osw = new OutputStreamWriter(fos, charsetName);
			bw = new BufferedWriter(osw);
			for (String str : datas)
			{
				bw.write(str);
				bw.write(NEW_LINE);
			}

			// 解锁
			fileLock.release();
		}
		catch (Exception e)
		{
			System.out.println(IOHelper.getTraceMessage(e));
		}
		finally
		{
			if (fileLock != null)
			{
				try
				{
					fileLock.release();
				}
				catch (IOException e)
				{
				}
			}

			if (bw != null)
			{
				try
				{
					bw.close();
				}
				catch (IOException e)
				{
				}
			}

			if (osw != null)
			{
				try
				{
					osw.close();
				}
				catch (IOException e)
				{
				}
			}

			if (fos != null)
			{
				try
				{
					fos.close();
				}
				catch (IOException e)
				{
				}
			}

		}
	}

	public static void main(String[] args) throws Exception
	{
		FileStore store = new FileStore("E:\\temp\\1\\test.bcp", 5);

		store.Append(false, 1, 1, 2);
		store.Append(true);
		store.AddData(1, 1, 2, 3, "测试");
		store.AddData(2, 2, 2, 3, 4);
		store.AddData(3, 3, 2, 3, 4);
		store.AddData(4, 4, 2, 3, 4);
		store.AddData(5, 1, 2, 3, 4);
		store.AddData(6, 2, 2, 3, 4);
		store.AddData(7, 3, 2, 3, 4);
		store.AddData(8, 4, 2, 3, 4);

		TestThread t1 = store.new TestThread(store, 10);
		TestThread t2 = store.new TestThread(store, 20);
		t1.start();
		t2.start();

		Thread.sleep(3000);

		store.Flush();

	}

	class TestThread extends Thread
	{
		FileStore store = null;
		int flag = 0;

		public TestThread(FileStore store, int flag)
		{
			this.store = store;
			this.flag = flag;
		}

		@Override
		public void run()
		{
			for (int i = 0; i < 1000; i++)
			{
				store.AddData(flag, 1, 2, 3, "测试");
			}
		}
	}

}
