package cn.mastercom.bigdata.conf.config;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader.LineHandler;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

public class CellBuildWifi
{
	private String cellBuildWifiPath = "";
	private HashMap<String, Integer> cellBuildWifiMap;
	private String mroXdrMergePath;

	public CellBuildWifi() throws IOException
	{
		cellBuildWifiPath = MainModel.GetInstance().getAppConfig().getCellBuildWifiPath();
		cellBuildWifiMap = new HashMap<String, Integer>();
		mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
	}

	public static void main(String args[])
	{
		try
		{
			CellBuildWifi tempcellbuild = new CellBuildWifi();
			tempcellbuild.loadWifi(null, "C:\\Users\\xsh\\Desktop\\cell_build_wifi_2301_74451261.bcp.gz", false);
			for (String key : tempcellbuild.cellBuildWifiMap.keySet())
			{
				System.out.print(key + ":");
				System.out.println(tempcellbuild.cellBuildWifiMap.get(key));
			}
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public boolean loadBuildWifi(Configuration conf, long eci, int cityid)
	{
		String path = cellBuildWifiPath + "/cell_build_wifi_" + cityid + "_" + eci + ".bcp.gz";
		return loadWifi(conf, path, false);//所有地市配置改成明文
	}

	public boolean loadWifi(Configuration conf, String filePath, boolean binary)
	{
		try {
			FileReader.readFile(conf, filePath, new LineHandler() {
				
				@Override
				public void handle(String line) {
					loadCellBuildWifi(line);
				}
			});
		} catch (Exception e1) {
			LOGHelper.GetLogger().writeLog(LogType.error,"loadcellBuildWifi error", "loadcellBuildWifi error " + filePath, e1);
			return false;
		}
//		DataInputStream reader = null;
//		HDFSOper hdfsOper = null;
//		String filename = "";
//		try
//		{
//			if (!filePath.contains(":"))
//			{
//				hdfsOper = new HDFSOper(conf);
//				filename = filePath.substring(filePath.lastIndexOf("/")+1);
//				hdfsOper.mkfile(mroXdrMergePath+"/cellFlag/"+filename);
//				if (!hdfsOper.checkFileExist(filePath))
//				{
//					LOGHelper.GetLogger().writeLog(LogType.error, "cellBuildWifi config is not exists: " + filePath);
//					return false;
//				}
//				reader = new DataInputStream(new GZIPInputStream(hdfsOper.getHdfs().open(new Path(filePath))));
//			}
//			else if (filePath.endsWith(".gz"))
//			{
//				reader = new DataInputStream(new GZIPInputStream(new FileInputStream(filePath)));
//			}
//			else
//			{
//				reader = new DataInputStream(new FileInputStream(filePath));
//			}
//			
//			if (binary)
//			{
//				byte[] ssid = new byte[6];
//				int eci = reader.readInt();
//				int count = reader.readInt();
//				for (int i = 0; i < count; i++)
//				{
//					int buildid = reader.readInt();
//					int length = reader.readInt();
//					for (int j = 0; j < length; j++)
//					{
//						int level = reader.readShort();
//						reader.readFully(ssid);
//						String mac = bytesToHexString(ssid);
//						String key = buildid + "_" + mac;
//						cellBuildWifiMap.put(key, level);
//					}
//				}
//			}
//			else
//			{
//				String value = "";
//				while((value=reader.readLine())!=null)
//				{
//					loadCellBuildWifi(value);
//				}
//			}
//			
//		}
//		catch (Exception e)
//		{
//			LOGHelper.GetLogger().writeLog(LogType.error, "loadcellBuildWifi error " + filePath, e);
//			return false;
//		}
//		finally
//		{
//			try
//			{
//				hdfsOper.delete(mroXdrMergePath+"/cellFlag/"+filename);
//				if (reader != null)
//				{
//					reader.close();
//				}
//			}
//			catch (Exception e)
//			{
//
//			}
//		}
		return true;
	}
	
	public void loadCellBuildWifi(String value)
	{
		try
		{
			String[] strs = value.split("\t", -1);
			String buildid = strs[2];
			int level = Integer.parseInt(strs[3]);
			String mac = strs[4];
			cellBuildWifiMap.put(buildid + "_" + mac, level);
		}
		catch (Exception e)
		{
			// TODO: handle exception
		}
		
	}

	public static String bytesToHexString(byte[] src)
	{
		StringBuilder stringBuilder = new StringBuilder("");
		if (src == null || src.length <= 0)
		{
			return null;
		}
		for (int i = 0; i < src.length; i++)
		{
			int v = src[i] & 0xFF;
			String hv = Integer.toHexString(v);
			if (hv.length() < 2)
			{
				stringBuilder.append(0);
			}
			stringBuilder.append(hv);
		}
		return stringBuilder.toString();
	}

	public HashMap<String, Integer> getCellBuildWifiMap()
	{
		return cellBuildWifiMap;
	}

	public int getLevel(int buildid, String mac)
	{
		String key = buildid + "_" + mac;
		if (cellBuildWifiMap.get(key) != null)
		{
			return cellBuildWifiMap.get(key) * 3;
		}
		return -1;
	}
}
