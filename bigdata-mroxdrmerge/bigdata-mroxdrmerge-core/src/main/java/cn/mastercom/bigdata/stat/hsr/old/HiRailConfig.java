package cn.mastercom.bigdata.stat.hsr.old;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import cn.mastercom.bigdata.StructData.SIGNAL_XDR_4G;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

public class HiRailConfig
{
	public static HashMap<Long, double[]> cellMap;

	public static HashMap<String, double[]> railGrid;// 存放高铁栅格

	public static ArrayList<HashMap<Long, double[]>> cellMapList;
	public static ArrayList<HashMap<String, double[]>> railMapList;
	public static HashMap<String, double[]> rruMap;
	public static StringBuffer errLog = new StringBuffer();
	
	
	public static void main(String[] args)
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", args[0]);
		loadConfig(conf);
	}

	public static boolean loadConfig(Configuration conf)
	{
		cellMapList = new ArrayList<HashMap<Long, double[]>>();
		railMapList = new ArrayList<HashMap<String, double[]>>();
		HDFSOper hdfsOper;
		Path path = null;
		FileStatus[] cellPathList = null;
		FileStatus[] RailPathList = null;
		
		try
		{
			hdfsOper = new HDFSOper(conf);

			String cellPathString = MainModel.GetInstance().getAppConfig().getRailCellConf();
			path = new Path(cellPathString);
			if (hdfsOper.checkDirExist(cellPathString))
			{
				cellPathList = hdfsOper.getFs().listStatus(path);
			}
			else
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "RailCellConf not exist");
				return false;
			}
			path = new Path(MainModel.GetInstance().getAppConfig().getRailConf());
			if (hdfsOper.checkDirExist(MainModel.GetInstance().getAppConfig().getRailCellConf()))
			{
				RailPathList = hdfsOper.getFs().listStatus(path);
			}
			else
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "RailConf not exist");
				return false;
			}

			for (int i = 0; i < cellPathList.length; i++)
			{
				HashMap<Long, double[]> theCellMap = new HashMap<Long, double[]>();
				String cellPath = cellPathList[i].getPath().toString();
				FSDataInputStream inputStream = hdfsOper.getInputStream(cellPath);
				BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
				String line = "";
				while ((line = br.readLine()) != null)
				{
					String[] split = line.split("\t");

					if (split.length == 5)
					{
						try
						{
							theCellMap.put(Long.parseLong(split[0]),
									new double[] { Double.parseDouble(split[1]), Double.parseDouble(split[2]),
											Double.parseDouble(split[3]), Double.parseDouble(split[4]) });
						}
						catch (Exception e)
						{
							errLog.append(e.getMessage() + "\r\n");
							return false;
						}
					}
				}
				cellMapList.add(theCellMap);

				String railPathString = RailPathList[i].getPath().toString();
				FSDataInputStream inputStream1 = hdfsOper.getInputStream(railPathString);
				BufferedReader br1 = new BufferedReader(new InputStreamReader(inputStream1));

				HashMap<String, double[]> theRailGrid = new HashMap<String, double[]>();
				line = "";
				while ((line = br1.readLine()) != null)
				{
					String[] split = line.split("\t");
					if (split.length == 5)
					{
						theRailGrid.put(split[0],
								new double[] { Double.parseDouble(split[1]), Double.parseDouble(split[2]),
										Double.parseDouble(split[3]), Double.parseDouble(split[4]) });
					}
				}
				railMapList.add(theRailGrid);
				br.close();
				br1.close();
				inputStream.close();
				inputStream1.close();

			}

			if (!hdfsOper.checkFileExist(MainModel.GetInstance().getAppConfig().getRailStation()))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "RailStation not exist");
			}
			FSDataInputStream inputStream2 = hdfsOper
					.getInputStream(MainModel.GetInstance().getAppConfig().getRailStation());
			BufferedReader br2 = new BufferedReader(new InputStreamReader(inputStream2));
			String line = "";
			RailFillFunc.railtaimap = new HashMap<>();
			while ((line = br2.readLine()) != null)
			{
				String[] split = line.split("\t");
				RailFillFunc.railtaimap.put(split[0],
						new double[] { Double.parseDouble(split[1]), Double.parseDouble(split[2]) });
			}
			
			/*-------------------------------------*/
			FSDataInputStream inputStream3 = hdfsOper.getInputStream(MainModel.GetInstance().getAppConfig().getRailRRU());
			BufferedReader br3 = new BufferedReader(new InputStreamReader(inputStream3));
			line = "";
			rruMap = new HashMap<>();
			while ((line = br3.readLine()) != null)
			{
				String[] split = line.split("\t");
				rruMap.put(split[0] + "," + split[1],
						new double[] { Double.parseDouble(split[2]), Double.parseDouble(split[3]) });
			}
			

			DataAdapterConf.ParseItem parseItem_MME = MainModel.GetInstance().getDataAdapterConfig()
					.getParseItem("S1-MME");
			RailFillFunc.dataAdapterReader_MME = new DataAdapterReader(parseItem_MME);

			br2.close();
			br3.close();
			inputStream2.close();
			inputStream3.close();

		}
		catch (Exception e1)
		{
			e1.printStackTrace();
			// TODO Auto-generated catch block
			errLog.append(e1.getMessage() + "\r\n");
			return false;
		}

		for (int j = 0; j < cellMapList.size(); j++)
		{
			LOGHelper.GetLogger().writeLog(LogType.info, "cellMapList" + j + ".size:" + cellMapList.get(j).size());
		}
		for (int j = 0; j < railMapList.size(); j++)
		{
			LOGHelper.GetLogger().writeLog(LogType.info, "railMapList" + j + ".size:" + railMapList.get(j).size());
		}
		LOGHelper.GetLogger().writeLog(LogType.info, "RailFillFunc.railtaimap.size:" + RailFillFunc.railtaimap.size());

		return true;
	}

}
