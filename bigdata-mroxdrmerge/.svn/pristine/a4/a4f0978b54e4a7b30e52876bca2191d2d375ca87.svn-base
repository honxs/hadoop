package cn.mastercom.bigdata.standalone.local;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

public class FileReader
{
	public interface LineHandler
	{
		void handle(String line);
	}

	public interface DataHandler
	{
		void handle(DataInputStream dataInputStream) throws Exception;
	}

	public interface StreamHandler
	{
		void handle(InputStream inputStream) throws Exception;
	}

	public static void readFileUTF8(String file, final LineHandler lineHandler) throws Exception
	{
		readFile(file, lineHandler, "utf-8");
	}

	public static void readFileGBK(String file, final LineHandler lineHandler) throws Exception
	{
		readFile(file, lineHandler, "gbk");
	}

	public static void readFile(String file, final LineHandler lineHandler, final String encoding) throws Exception
	{

		readFile(file, new StreamHandler()
		{
			@Override
			public void handle(InputStream inputStream) throws Exception
			{
				readStreamLine(inputStream, lineHandler, encoding);
			}
		});

	}

	public static void readFileGzUTF8(String file, final LineHandler lineHandler) throws Exception
	{
		readFileGz(file, lineHandler, "utf-8");
	}

	public static void readFileGzGBK(String file, LineHandler lineHandler) throws Exception
	{
		readFileGz(file, lineHandler, "gbk");
	}

	public static void readFileGz(String file, final LineHandler lineHandler, final String encoding) throws Exception
	{
		readFile(file, new StreamHandler()
		{
			@Override
			public void handle(InputStream inputStream) throws Exception
			{
				readStreamGz(inputStream, new StreamHandler()
				{
					@Override
					public void handle(final InputStream inputStream) throws Exception
					{
						readStreamLine(inputStream, lineHandler, encoding);
					}
				});
			}
		});
	}

	public static void readFile(String file, final DataHandler dataHandler) throws Exception
	{

		readFile(file, new StreamHandler()
		{
			@Override
			public void handle(InputStream inputStream) throws Exception
			{
				readStreamData(inputStream, dataHandler);
			}
		});

	}

	public static void readFileGz(String file, final DataHandler dataHandler) throws Exception
	{

		readFile(file, new StreamHandler()
		{
			@Override
			public void handle(InputStream inputStream) throws Exception
			{
				readStreamGz(inputStream, new StreamHandler()
				{

					@Override
					public void handle(InputStream inputStream) throws Exception
					{
						readStreamData(inputStream, dataHandler);

					}
				});
			}
		});

	}

	private static void readFile(String file, final StreamHandler streamHandler) throws Exception
	{
		FileInputStream fis = null;

		try
		{
			fis = new FileInputStream(file);
			streamHandler.handle(fis);
		}
		catch (Exception e)
		{
			throw e;
		}
		finally
		{
			if (fis != null)
			{
				try
				{
					fis.close();
				}
				catch (IOException e)
				{
				}
			}
		}

	}

	private static void readStreamGz(InputStream fis, final StreamHandler streamHandler) throws Exception
	{
		GZIPInputStream gis = null;
		try
		{
			gis = new GZIPInputStream(fis);
			streamHandler.handle(gis);
		}
		catch (Exception e)
		{
			throw e;
		}
		finally
		{
			if (gis != null)
			{
				try
				{
					gis.close();
				}
				catch (IOException e)
				{
				}
			}
		}
	}

	private static void readStreamData(InputStream fis, final DataHandler dataHandler) throws Exception
	{
		DataInputStream dis = null;

		try
		{
			dis = new DataInputStream(fis);
			dataHandler.handle(dis);
		}
		catch (Exception e)
		{
			throw e;
		}
		finally
		{
			if (dis != null)
			{
				try
				{
					dis.close();
				}
				catch (IOException e)
				{
				}
			}
		}

	}

	private static void readStreamLine(InputStream fis, final LineHandler lineHandler, String encoding) throws Exception
	{
		InputStreamReader isr = null;
		BufferedReader br = null;

		try
		{
			isr = new InputStreamReader(fis, encoding);
			br = new BufferedReader(isr);

			String line = "";
			while ((line = br.readLine()) != null)
			{
				lineHandler.handle(line);
			}
		}
		catch (Exception e)
		{
			throw e;
		}
		finally
		{
			if (br != null)
			{
				try
				{
					br.close();
				}
				catch (IOException e)
				{
				}
			}

			if (isr != null)
			{
				try
				{
					isr.close();
				}
				catch (IOException e)
				{
				}
			}

		}
	}
}
