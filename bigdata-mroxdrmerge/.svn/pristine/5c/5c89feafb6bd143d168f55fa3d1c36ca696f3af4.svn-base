package cn.mastercom.bigdata.mapr;
import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

public class Main_Hour
{
	public static void main(String args[])
	{
		Configuration conf = new Configuration();
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
		}
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.SuYanPlat))
		{
			String id = MainModel.GetInstance().getAppConfig().getSuYanId();
			String key = MainModel.GetInstance().getAppConfig().getSunYanKey();
			String queue = MainModel.GetInstance().getAppConfig().getSuYanQueue();
			conf.set("hadoop.security.bdoc.access.id", id);
			conf.set("hadoop.security.bdoc.access.key", key);
			conf.set("mapreduce.job.queuename", queue);
		}
		HDFSOper hdfsOper = null;
		try
		{
			hdfsOper = new HDFSOper(conf);
		}
		catch (Exception e1)
		{
			System.out.println("create HDFSOper error !");
		}
		String queueName = args[0];// network
		String statTime = args[1];// 01_151013
		String xdrDataPath_mme = args[2];
		String xdrDataPath_http = args[3];
		String xdrLocationPath = args[4];
		String statType = args.length >= 6 ? args[5] : "";
		int hoursNum = Integer.parseInt(args[6]);
		String MergeStat = "";
		if (args.length < 7)
		{
			System.out.println("args num is not right !");
			return;
		}
		else if (args.length > 7)
		{
			MergeStat = args[7];
		}

		String mmeKeyWord = "";
		String mmeFilter = "";
		String httpKeyWord = "";
		String httpFilter = "";
		String mrKeyWord = "";
		String mrFilter = "";
		String locKeyWord = "NULL";
		String locFilter = "NULL";
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan))
		{
			mmeKeyWord = "NULL";
			mmeFilter = "NULL";
			httpKeyWord = "NULL";
			httpFilter = "NULL";
			mrKeyWord = "MRO";
		}
		else if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			mmeKeyWord = "NULL";
			mmeFilter = "NULL";
			httpKeyWord = "NULL";
			httpFilter = "NULL";
			mrKeyWord = "NULL";
			mrFilter = "NULL";

		}
		else if (MainModel.GetInstance().getCompile().Assert(CompileMark.HaErBin))
		{
			mmeKeyWord = "_S1MME_";
			httpKeyWord = "HTTP";
			mrKeyWord = "MR";
		}
		String[] var = new String[15];
		String day = statTime.replace("01_", "");
		var[0] = queueName;
		var[3] = xdrDataPath_http;
		var[4] = xdrLocationPath;
		var[5] = statType;
		var[6] = mmeKeyWord;
		var[7] = mmeFilter;
		var[8] = httpKeyWord;
		var[9] = httpFilter;
		var[10] = mrKeyWord;
		var[12] = locKeyWord;
		var[13] = locFilter;
		var[14] = "";

		int times = 24 / hoursNum;
		for (int i = 0; i < times; i++)
		{
			String filterMr = "";
			var[1] = getDateHour(statTime, i * hoursNum);
			var[2] = "";// 新一轮清空mme
			var[3] = "";// 新一轮清空http
			var[14] = "";// 新一轮清空mro
			int count = 0;
			for (int j = 0; j < hoursNum; j++)
			{
				count = i * hoursNum + j;
				filterMr += getDateHour(statTime, count) + ",";
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan))
				{
					// mmepath
					var[2] += getStringPath("/sichuan/ods/ns/s1_mme/20%1$s/%2$s,", day, count);
					// httppath
					var[3] = "NULL";
				}
				else if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
				{
					// mme path
					var[2] += getStringPath("/user/wangjun/S_O_DPI_LTE_S1_MME/load_time_d=20%1$s/load_time_h=%2$s/$LIST,", day, count);
					// http path
					var[3] += getStringPath("/user/wangjun/S_O_DPI_LTE_S1U_HTTP/load_time_d=20%1$s/load_time_h=%2$s/$LIST,", day, count);
					// mropath
					var[14] += getStringPath("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20%1$s/load_time_h=%2$s/$LIST/$LIST,", day, count);
				}
				else if (MainModel.GetInstance().getCompile().Assert(CompileMark.HaErBin))
				{
					var[2] = xdrDataPath_mme;
					var[3] = xdrDataPath_http;
				}
			}
			mrFilter = filterMr.replace("01_", "");
			if (var[2].length() > 0 && var[2].endsWith(","))
			{
				var[2] = var[2].substring(0, var[2].length() - 1);
			}
			if (var[3].length() > 0 && var[3].endsWith(","))
			{
				var[3] = var[3].substring(0, var[3].length() - 1);
			}
			if (mrFilter.length() > 0 && mrFilter.endsWith(","))
			{
				// filters
				var[7] = mrFilter.substring(0, mrFilter.length() - 1);
				var[9] = mrFilter.substring(0, mrFilter.length() - 1);
				var[11] = mrFilter.substring(0, mrFilter.length() - 1);
			}
			if (var[14].endsWith(","))
			{
				var[14] = var[14].substring(0, var[14].length() - 1);
			}

			var[2] = hdfsOper.getPackageFolders(var[2]).length() == 0 ? "NULL" : var[2];
			var[3] = hdfsOper.getPackageFolders(var[3]).length() == 0 ? "NULL" : var[3];
			var[14] = hdfsOper.getPackageFolders(var[14]).length() == 0 ? "NULL" : var[14];

			try
			{
				System.out.println("");
				for (String temp : var)
				{
					System.out.print(temp);
					System.out.print("\t");
				}
				Main.main(var);
			}
			catch (Exception e)
			{
			}
		}
		// 汇聚
		if (MergeStat.equals("MERGESTATALL"))
		{
			FileMoverNew.doMergestat(statTime, queueName);
		}

		String date = statTime.substring(0, 9);
		String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
		String finshPath = mroXdrMergePath + "/MyFlag/data_" + date + "/tb_outflag_" + date;
		String finshFilePath = finshPath + "/SUCCESS.txt";
		if (!hdfsOper.checkFileExist(finshFilePath))
		{
			hdfsOper.mkdir(finshPath);
			hdfsOper.mkfile(finshFilePath);
		}
	}

	/**
	 * 根据hour组好Stringpath
	 * 
	 * @param formatString
	 * @param date
	 * @param hour
	 * @return
	 */
	public static String getStringPath(String formatString, String date, int hour)
	{
		if (hour < 10)
		{
			return String.format(formatString, date, "0" + hour);
		}
		else
		{
			return String.format(formatString, date, "" + hour);
		}
	}

	/*
	 * 根据小时获取日期时间
	 */
	public static String getDateHour(String data, int hour)
	{
		if (hour < 10)
		{
			return data + "0" + hour;
		}
		else
		{
			return data + hour;
		}
	}
}
