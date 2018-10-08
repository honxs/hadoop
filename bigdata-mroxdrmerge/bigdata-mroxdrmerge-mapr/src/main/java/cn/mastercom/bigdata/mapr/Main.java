package cn.mastercom.bigdata.mapr;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Job;

import com.huawei.bigdata.mapreduce.local.lib.SecurityUtils;

import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.mapr.evt.locall.LocAllEXMain;
import cn.mastercom.bigdata.mapr.evt.locall.LocAllExUtil;
import cn.mastercom.bigdata.mapr.mdt.loc.MdtLabelFillMain;
import cn.mastercom.bigdata.mapr.mro.loc.MroLableFillMains;
import cn.mastercom.bigdata.mapr.stat.freqloc.FreqLocMain;
import cn.mastercom.bigdata.mapr.stat.imsifill.LocImsiFillMain;
import cn.mastercom.bigdata.mapr.stat.userAna.UserAnaAllMain;
import cn.mastercom.bigdata.mapr.stat.userResident.UserResidentMain;
import cn.mastercom.bigdata.mapr.stat.userResident.buildIndoorCell.BuildIndoorCellMain;
import cn.mastercom.bigdata.mapr.stat.userResident.homeBroadbandLoc.MergeUserResidentMain;
import cn.mastercom.bigdata.mapr.stat.villagestat.VillageStatMain;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.mapr.xdr.loc.XdrLabelFillMain;
import cn.mastercom.bigdata.mapr.xdr.loc.prepare.XdrPrepareMain;
import cn.mastercom.bigdata.mergestat.deal.MergeStatTablesEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsOTTTableEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.XdrLocTablesEnum;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userAna.tableEnums.HsrEnums;
import cn.mastercom.bigdata.stat.userResident.enmus.ResidentLocTablesEnum;
import cn.mastercom.bigdata.util.HdfsHelper;
import cn.mastercom.bigdata.util.StringUtil;
import cn.mastercom.bigdata.util.TimeUtil;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileMatcher;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileWriter;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.TmpFileFilter;
import cn.mastercom.bigdata.xdr.prepare.stat.XdrPrepareTablesEnum;

public class Main
{
	
	private static final Log LOG = LogFactory.getLog(Main.class);
	
	private static Map<String, Integer> statTypeMap = new HashMap<String, Integer>();
	private static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmm");
	/**
	 * 日期格式提取器，日期格式写在配置文件中
	 * 例如：/mt_wlyh/Data/mroxdrmerge/mro_loc/date_01_${yyMMdd}/test
	 */
	private static final String dateFormatGetter = "\\$\\{[yMdHms-]+\\}";

	public static void main(String[] args) throws Exception
	{
		if (args.length < 6)
		{
			LOG.info("args num is not right. " + args.length);
			return;
		}

		String mmeKeyWord = "";
		String mmeFilter = "";
		String httpKeyWord = "";
		String httpFilter = "";
		String mrKeyWord = "";
		String mrFilter = "";
		String locKeyWord = "";
		String locFilter = "";

		if (args.length >= 14)
		{
			if (!args[6].toUpperCase().equals("NULL") && !args[7].toUpperCase().equals("NULL"))
			{
				mmeKeyWord = args[6];
				mmeFilter = args[7];
			}
			if (!args[8].toUpperCase().equals("NULL") && !args[9].toUpperCase().equals("NULL"))
			{
				httpKeyWord = args[8];
				httpFilter = args[9];
			}
			if (!args[10].toUpperCase().equals("NULL") && !args[11].toUpperCase().equals("NULL"))
			{
				mrKeyWord = args[10];
				mrFilter = args[11];
			}
			if (!args[12].toUpperCase().equals("NULL") && !args[13].toUpperCase().equals("NULL"))
			{
				locKeyWord = args[12];
				locFilter = args[13];
			}
		}

		// 初始化Hadoop系统环境
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
		MainModel.GetInstance().setConf(conf);

		//校验输入参数
		String queueName = args[0];// network
		String statTime = args[1];// 01_151013
		String xdrDataPath_mme = args[2];
		String xdrDataPath_http = args[3];
		String xdrLocationPath = args[4];

		String statType = args.length >= 6 ? args[5] : "";

		if (xdrDataPath_mme.length() == 0 || xdrDataPath_http.length() == 0)
		{
			LOG.info("xdr data path is null, check the inputs");
			return;
		}

		String[] statTypes = statType.split(",");
		statTypeMap.clear();
		if (statType.trim().length() > 0)
		{
			for (int i = 0; i < statTypes.length; ++i)
			{
				statTypes[i] = statTypes[i].toUpperCase();
				statTypeMap.put(statTypes[i], 0);
			}
		}

		//使用日期 来 格式化 所有的数据路径
		Date date = parseDate(statTime);

		xdrDataPath_mme = getRealFSPath(date, xdrDataPath_mme, conf);
		xdrDataPath_http = getRealFSPath(date, xdrDataPath_http, conf);
		xdrLocationPath = getRealFSPath(date, xdrLocationPath, conf);
		String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
//		String mroDataPath = getRealFSPath(date, MainModel.GetInstance().getAppConfig().getMroDataPath(), conf);
		String mtMroDataPath = getRealFSPath(date, MainModel.GetInstance().getAppConfig().getMTMroDataPath(), conf);
		String mreDataPath = getRealFSPath(date,  MainModel.GetInstance().getAppConfig().getMreDataPath(), conf);
		String huaweiMdtLogDataPath = getRealFSPath(date, MainModel.GetInstance().getAppConfig().getHUAWEIMdtLogDataPath(), conf);
		String huaweiMdtImmDataPath = getRealFSPath(date, MainModel.GetInstance().getAppConfig().getHUAWEIMdtImmDataPath(), conf);
		String zteMdtLogDataPath = getRealFSPath(date, MainModel.GetInstance().getAppConfig().getZTEMdtLogDataPath(), conf);
		String zteMdtImmDataPath = getRealFSPath(date, MainModel.GetInstance().getAppConfig().getZTEMdtImmDataPath(), conf);
		String locWFDataPath = getRealFSPath(date, MainModel.GetInstance().getAppConfig().getLocWFDataPath(), conf);
		String gsmCallPath = getRealFSPath(date, MainModel.GetInstance().getAppConfig().getGsmCallPath(), conf);
		String gsmLocationPath = getRealFSPath(date, MainModel.GetInstance().getAppConfig().getGsmLocationPath(), conf);
		String gsmSmsPath = getRealFSPath(date, MainModel.GetInstance().getAppConfig().getGsmSmsPath(), conf);
		String tdCallPath = getRealFSPath(date, MainModel.GetInstance().getAppConfig().getTdCallPath(), conf);
		String tdLocationPath = getRealFSPath(date, MainModel.GetInstance().getAppConfig().getTdLocationPath(), conf);
		String tdSmsPath = getRealFSPath(date, MainModel.GetInstance().getAppConfig().getGsmSmsPath(), conf);


		HDFSOper hdfsOper = null;
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
		}
		if (!mroXdrMergePath.contains(":") || MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			hdfsOper = new HDFSOper(conf);
		}

		LOG.info("<mroXdrMergePath>: " + mroXdrMergePath);
		LOG.info("<mtMroDataPath>: " + mtMroDataPath);
		LOG.info("<mreDataPath>: " + mreDataPath);
		LOG.info("<statType>: " + statType);
		TmpFileFilter.hdfsOper = hdfsOper;

		if (checkDoStat("XDRPREPARE"))
		{
			String[] myArgs = new String[8];
			myArgs[0] = "100";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = xdrDataPath_mme;
			myArgs[4] = xdrDataPath_http;
			myArgs[5] = mroXdrMergePath;
			myArgs[6] = mmeFilter;
			myArgs[7] = httpFilter;

			// 增加待处理文件过滤
			TmpFileFilter.mapValidStr.clear();
			if (mmeKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(mmeKeyWord, mmeFilter);
			}
			if (httpKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(httpKeyWord, httpFilter);
			}
			
			String _successFile = XdrPrepareTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3)) + "/output/_SUCCESS";
			if (hdfsOper == null || !hdfsOper.checkFileExist(_successFile))
			{
				Job xdrPrepareJob = XdrPrepareMain.CreateJob(myArgs);
				Date stime = new Date();

				if (!xdrPrepareJob.waitForCompletion(true))
				{
					LOG.info("xdr prepare Job error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", XdrPrepareTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3)), mins, timeFormat.format(stime), timeFormat.format(etime));
				if (hdfsOper != null && !hdfsOper.checkFileExist(timeFileName))
				{
					hdfsOper.mkfile(timeFileName);
				}
			}
			else
			{
				LOG.info("XDRPrepare has bend dealed succesfully:" + myArgs[5]);
			}
		}

		if (checkDoStat("LOCFILL"))
		{
			// 执行loc fill
			String[] myArgs = new String[10];
			myArgs[0] = "100";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = xdrLocationPath;
			myArgs[4] = xdrDataPath_http;
			myArgs[5] = mroXdrMergePath;
			myArgs[6] = mmeFilter;
			myArgs[7] = httpFilter;
			myArgs[8] = mmeKeyWord;
			myArgs[9] = httpKeyWord;
			// 增加待处理文件过滤
			TmpFileFilter.mapValidStr.clear();
			if (mmeKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(mmeKeyWord, mmeFilter);
			}
			if (httpKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(httpKeyWord, httpFilter);
			}
			String _successFile = XdrPrepareTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3)) + "/output/_SUCCESS";
			if (hdfsOper == null || !hdfsOper.checkFileExist(_successFile))
			{
				Job mroformatJob = LocImsiFillMain.CreateJob(myArgs);

				Date stime = new Date();

				if (!mroformatJob.waitForCompletion(true))
				{
					LOG.info("locFillJob error! stop run.");
					throw (new Exception("system.exit1"));
				}
				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", XdrPrepareTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3)), mins, timeFormat.format(stime), timeFormat.format(etime));
				if (hdfsOper != null && !hdfsOper.checkFileExist(timeFileName))
				{
					hdfsOper.mkfile(timeFileName);
				}
			}
			else
			{
				LOG.info("LOCFILL has bend dealed succesfully:" + myArgs[5]);
			}
		}
		
		/*
		 * 功能：实现MDT和MME数据进行imsi回填用户  
		 * 日期：2018-7-12
		 * Author:  hdr 
		 * checkDoStat : "MDTFILL"
		 */
		if (checkDoStat("MDTFILL")){
			String mdtargs[] = new String[9];
			mdtargs[0] = "100";
			mdtargs[1] = queueName;
			mdtargs[2] = statTime;      //这里的statTime是outpath_date
			mdtargs[3] = xdrDataPath_mme;
			mdtargs[4] = huaweiMdtImmDataPath;
			mdtargs[5] = huaweiMdtLogDataPath;
			mdtargs[6] = zteMdtImmDataPath;
			mdtargs[7] = zteMdtLogDataPath;
			mdtargs[8] = mroXdrMergePath;
			
			
			String _successFile = XdrPrepareTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3)) + "/output/_SUCCESS";
			if (hdfsOper == null || !hdfsOper.checkFileExist(_successFile))
			{
				Job MdtLabelFillJob = MdtLabelFillMain.CreateJob(mdtargs);
				Date stime = new Date();
				
				if (!MdtLabelFillJob.waitForCompletion(true))
				{
					LOG.info("MdtLabelFillJob error! stop run.");
					throw (new Exception("system.exit1"));
				}
				
				
				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", XdrPrepareTablesEnum.getBasePath(mroXdrMergePath,statTime.substring(3)), mins, timeFormat.format(stime), timeFormat.format(etime));
				if (hdfsOper != null && !hdfsOper.checkFileExist(timeFileName))
				{
					hdfsOper.mkfile(timeFileName);
				  }
			}
			else
			{
				LOG.info("MDTMMEFillBack has been dealed succesfully:" + XdrLocTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3)));
			}
			
		}

		if (checkDoStat("XDRLOC"))
		{
			String locPath = "";
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.URI_ANALYSE) || checkDoStat("MDTFILL"))
			{
				locPath = XdrPrepareTablesEnum.xdrLocation.getPath(mroXdrMergePath, statTime.substring(3));
			}
			if (locPath.length() > 0)
			{//
				if (!checkDoStat("LOCFILL"))
				{
					locPath = locPath + "," + xdrLocationPath;
				}
			}
			else
			{
				locPath = xdrLocationPath;
			}

			// 增加待处理文件过滤
			TmpFileFilter.mapValidStr.clear();
			if (mmeKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(mmeKeyWord, mmeFilter);
			}
			if (httpKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(httpKeyWord, httpFilter);
			}
			if (locKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(locKeyWord, locFilter);
			}
			// 执行xdr运算
			String[] myArgs = new String[21];
			// myArgs[0] = "3000";
			myArgs[0] = "1000";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = xdrDataPath_mme;
			myArgs[4] = xdrDataPath_http;

			myArgs[5] = String.format("/flume/23G/location/%s", "20" + statTime.substring(3));// 23g
			// path
			myArgs[6] = String.format("%s/xdrcellmark", mroXdrMergePath);
			myArgs[7] = locPath;
			myArgs[8] = locWFDataPath;
			myArgs[9] = String.format("%s/xdr_prepare/data_%s/TB_IMSI_COUNT_%s", mroXdrMergePath, statTime, statTime);
			myArgs[10] = mroXdrMergePath;

			myArgs[11] = gsmCallPath;
			myArgs[12] = gsmLocationPath;
			myArgs[13] = gsmSmsPath;
			myArgs[14] = tdCallPath;
			myArgs[15] = tdLocationPath;
			myArgs[16] = tdSmsPath;

			myArgs[17] = mmeFilter;
			myArgs[18] = httpFilter;
			myArgs[19] = mmeKeyWord;
			myArgs[20] = httpKeyWord;
			String _successFile = XdrLocTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3)) + "/output/_SUCCESS";
			LOG.info("程序执行结束的文件的路径为: " + _successFile);
			if (hdfsOper == null || !hdfsOper.checkFileExist(_successFile))
			{
				Job xdrLocJob = XdrLabelFillMain.CreateJob(myArgs);
				Date stime = new Date();

				if (!xdrLocJob.waitForCompletion(true))
				{
					LOG.info("xdrLocJob error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", XdrLocTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3)), mins, timeFormat.format(stime), timeFormat.format(etime));
				if(hdfsOper != null)
				{
					hdfsOper.mkfile(timeFileName);
				}
			}
			else
			{
				LOG.info("XDRLOC has been dealed succesfully:" + XdrLocTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3)));
			}
		}
		// 高铁-用户分析
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail2) && checkDoStat("HSR_USER_ANA"))
		{
			String _successFile = "";
			//判断存在就不跑
			_successFile = HsrEnums.getBasePath(mroXdrMergePath, statTime.substring(3)) + "/output/_SUCCESS";
			if (!hdfsOper.checkFileExist(_successFile))
			{
				String[] myArgs = new String[4];
				myArgs[0] = queueName;
				myArgs[1] = statTime;
				myArgs[2] = XdrLocTablesEnum.xdrLocation.getPath(mroXdrMergePath, statTime.substring(3));
				myArgs[3] = mroXdrMergePath;

				if(!UserAnaAllMain.run(myArgs))
				{
					LOG.info("hsrUserAna error! stop run.");
					throw (new Exception("system.exit1"));
				}

			}

		}

		if (checkDoStat("MROLOC_NEW"))
		{
			TmpFileFilter.mapValidStr.clear();
			if (mrKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(mrKeyWord, mrFilter);
			}
			// 执行mro运算
			String[] myArgs = new String[13];
			myArgs[0] = "10";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = XdrLocTablesEnum.xdrLocation.getPath(mroXdrMergePath, statTime.substring(3));
			myArgs[4] = mtMroDataPath;
			myArgs[5] = mroXdrMergePath;
			myArgs[6] = mreDataPath;
			myArgs[7] = mrFilter;
			myArgs[8] = huaweiMdtLogDataPath;
			myArgs[9] = huaweiMdtImmDataPath;
			myArgs[10] = zteMdtLogDataPath;
			myArgs[11] = zteMdtImmDataPath;
			myArgs[12] = mrKeyWord;
			
			String _successFile = MroCsOTTTableEnum.getBasePath(mroXdrMergePath, statTime.substring(3)) + "/output/_SUCCESS";
			if (mroXdrMergePath.contains(":") || !hdfsOper.checkFileExist(_successFile))
			{
				Job mroLocJob = MroLableFillMains.CreateJob(myArgs);

				long stime = System.currentTimeMillis();

				if (!mroLocJob.waitForCompletion(true))
				{
					LOG.info("mroLocJob error! stop run.");
					throw (new Exception("system.exit1"));
				}

				long etime = System.currentTimeMillis();
				int mins = (int) (etime / 1000L - stime / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", MroCsOTTTableEnum.getBasePath(mroXdrMergePath, statTime.substring(3)), mins, timeFormat.format(stime), timeFormat.format(etime));
				if (hdfsOper != null && !hdfsOper.checkFileExist(timeFileName))
				{
					hdfsOper.mkfile(timeFileName);
				}
			}
			else
			{
				LOG.info("MROLOC_NEW has bend dealed succesfully:" + MroCsOTTTableEnum.getBasePath(mroXdrMergePath, statTime.substring(3)));
			}
		}

		if (checkDoStat("FREQ_LOC"))
		{
			String[] myArgs = new String[4];
			myArgs[0] = "100";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = String.format("%s/freqloc/data_%s", mroXdrMergePath, statTime);

			String _successFile = myArgs[3] + "/output/_SUCCESS";
			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job freqLocMainJob = FreqLocMain.CreateJob(myArgs);
				Date stime = new Date();

				if (!freqLocMainJob.waitForCompletion(true))
				{
					LOG.info("freqLocMainJob error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[3], mins, timeFormat.format(stime), timeFormat.format(etime));
				if (hdfsOper != null && !hdfsOper.checkFileExist(timeFileName))
				{
					hdfsOper.mkfile(timeFileName);
				}
			}
			else
			{
				LOG.info("freqLoc has bend dealed succesfully:" + myArgs[3]);
			}
		}

		if (checkDoStat("MROVILLAGESTAT"))
		{
			// 执行mro village运算
			String[] myArgs = new String[7];
			myArgs[0] = "1000";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = String.format("/mt_wlyh/Data/config/village_grid", statTime);
			// myArgs[4] =
			// String.format("%1$s/mroformat/data_%2$s/mroformat_%2$s",
			// mroXdrMergePath, statTime);
			myArgs[4] = mtMroDataPath;
			myArgs[5] = String.format("%s/mro_village/data_%s", mroXdrMergePath, statTime);
			myArgs[6] = mreDataPath;
			String _successFile = myArgs[5] + "/output/_SUCCESS";

			if (mroXdrMergePath.contains(":") || !hdfsOper.checkFileExist(_successFile))
			{
				Job mroLocJob = VillageStatMain.CreateJob(myArgs);

				Date stime = new Date();

				if (!mroLocJob.waitForCompletion(true))
				{
					LOG.info("mroLocJob error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[5], mins, timeFormat.format(stime), timeFormat.format(etime));
				if (hdfsOper != null) {
					hdfsOper.mkfile(timeFileName);
				}
			}
			else
			{
				LOG.info("MROVILLAGESTAT has bend dealed succesfully:" + myArgs[5]);
			}
		}
		///////////////////////////////////////////////// MERGE STAT
		if (checkDoStat("MERGESTAT4"))
		{
			Mergestat_Round.doMergestat4(queueName, statTime, mroXdrMergePath, conf, hdfsOper);
		}

		if (checkDoStat("XDRLOCALL"))
		{
			String _successFile = "";
			//判断存在就不跑
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
			{   
				_successFile = mroXdrMergePath+"/xdr_locall/data_01_20"+
						statTime.substring(3)+"/imsi/output/_SUCCESS";
			}else
			{
				_successFile = mroXdrMergePath+"/xdr_locall/data_"+statTime+"/imsi/output/_SUCCESS";
			}

			if (!hdfsOper.checkFileExist(_successFile))
			{
				// 增加待处理文件过滤
				if (mmeKeyWord.length() > 0)
				{
					TmpFileFilter.mapValidStr.put(mmeKeyWord, mmeFilter);
				}
				if (httpKeyWord.length() > 0)
				{
					TmpFileFilter.mapValidStr.put(httpKeyWord, httpFilter);
				}
				/*
				 * 四川增加按照小时跑。例如6小时跑一次,四川比较特殊，位置库又没有按照小时分，我又要按照小时跑
				 */
				if(MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
					int i =0;
					int durTime = MainModel.GetInstance().getAppConfig().getTimeSchedule();
					while (i<24){
                        LocAllEXMain.step_up = i;
						LocAllEXMain.step_down = i+durTime;
						i=i+durTime;
						LocAllEXMain.localMain(args);
					}
				}else {
					LocAllEXMain.localMain(args);
				}
			}

		}

		if (checkDoStat("MERGESTATALL"))
		{   
			String srcPath = MergeStatTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3));
			String _successFile = srcPath + "/Finished";
			String tarPath = "";
			if (!mroXdrMergePath.contains(":") && !hdfsOper.checkFileExist(_successFile))
			{
				HdfsHelper.reNameExistsPath(hdfsOper, srcPath, tarPath);

				MergestatGroup.doMergestatGroup(queueName, statTime, mroXdrMergePath, conf, hdfsOper);

			}
			else
			{
				LOG.info("MERGESTATALL has bend dealed succesfully!");
			}
		}

		if (checkDoStat("RESIDENT_LOC"))
		{
			String[] myArgs = new String[4];
			myArgs[0] = "100";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = mroXdrMergePath;

			String _successFile1 = ResidentLocTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3)) + "/output/_SUCCESS";
			if (!hdfsOper.checkFileExist(_successFile1))
			{
				Job userResidentJob = UserResidentMain.CreateJob(myArgs);
				Date stime = new Date();

				if (!userResidentJob.waitForCompletion(true))
				{
					LOG.info("ResidentUser error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", ResidentLocTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3)), mins, timeFormat.format(stime), timeFormat.format(etime));
				if(hdfsOper != null) {
					hdfsOper.mkfile(timeFileName);
				}
			}
			else
			{
				LOG.info("residentUser has bend dealed succesfully:" + ResidentLocTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3)));
			}
		}

		if (checkDoStat("MERGE_RESIDENTUSER"))
		{
			String[] myArgs = new String[4];
			myArgs[0] = "100";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = String.format("%s/resident_user/data_%s", mroXdrMergePath, statTime);

			String _successFile = String.format("%s/output/_SUCCESS", myArgs[3]);
			if(!myArgs[3].contains(":") && !hdfsOper.checkFileExist(_successFile))
			{
				Job mergeUserResidentJob = MergeUserResidentMain.CreateJob(myArgs);
				Date stime = new Date();

				if (!mergeUserResidentJob.waitForCompletion(true))
				{
					LOG.info("mergeUserResidentJob  error! stop run.");
					throw (new Exception("system.exit1"));
				}
				
				String finishPath = String.format("%s/output", myArgs[3]);
				if (hdfsOper != null && hdfsOper.checkFileExist(finishPath))
				{
					hdfsOper.delete(finishPath);
					Job buildIndoorCellJob = BuildIndoorCellMain.CreateJob(myArgs);
					if (!buildIndoorCellJob.waitForCompletion(true))
					{
						LOG.info("buildIndoorCellJob  error! stop run.");
						throw (new Exception("system.exit1"));
					}
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[3], mins, timeFormat.format(stime), timeFormat.format(etime));
				if (hdfsOper != null && !hdfsOper.checkFileExist(timeFileName))
				{
					hdfsOper.mkfile(timeFileName);
				}
			}
			else
			{
				LOG.info("MERGE_RESIDENTUSER has bend dealed succesfully:" + myArgs[3]);
			}
		}

		// 计算完成后 新建一个完成标识目录
		String finishPath = mroXdrMergePath + "/MyFlag/data_" + statTime + "/tb_outflag_" + statTime;
		String finishFilePath = finishPath + "/SUCCESS.txt";
		String deleteSuccessPath = finishPath + "/DELETESUCCESS.txt";
		if (hdfsOper != null && !hdfsOper.checkFileExist(finishFilePath) && checkDoStat("MERGESTATALL") && args.length <= 14)// 大于14就是小时计算传过来的
		{
			hdfsOper.mkdir(finishPath);
			hdfsOper.mkfile(finishFilePath);
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.DeleteData))
			{
				//所有步骤执行完删除xdr_prepare,xdr_loc目录,以及xdr_locall,mro_loc中除带sample,err,hsr,LOC_LIB的数据，以减少吐出文件数
				String xdrpreparePath =  XdrPrepareTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3));
				String xdrlocPath =   XdrLocTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3));
				String xdrlocallImsiPath = String.format("%s/xdr_locall/data_%s/imsi", mroXdrMergePath, statTime);
				String xdrlocallS1apidPath = String.format("%s/xdr_locall/data_%s/s1apid", mroXdrMergePath, statTime);
				String mrolocnewPath = MroCsOTTTableEnum.getBasePath(mroXdrMergePath, statTime.substring(3));
				deleteData(hdfsOper, xdrpreparePath);
				deleteData(hdfsOper, xdrlocPath);
				deleteData(hdfsOper, xdrlocallImsiPath);
				deleteData(hdfsOper, xdrlocallS1apidPath);
				deleteData(hdfsOper, mrolocnewPath);
				hdfsOper.mkfile(deleteSuccessPath);
			}
			
			//统计输入文件大小
			final HDFSOper hdfsOperTmp = hdfsOper;
			/**
			 * 输入路径
			 * @author tanpeng
			 *
			 */
			 class InputPath{
					
					Date date;
					
					String dataType;
					
					String url;
					
					long size;

					public InputPath(Date date, String dataType, String url) {
						super();
						this.date = date;
						this.dataType = dataType;
						this.url = url;
						this.size = getPathSize(url, hdfsOperTmp);
					}
					
					private long getPathSize(String path, HDFSOper hdfsOper) {
						return MapReduceMainTools.getFileSize(path, hdfsOper);
					}
				}
			//组织写出的数据格式<日期	数据类型	文件大小>
			List<InputPath> fileList = new ArrayList<>();
			fileList.add(new InputPath(date, "Mro", mtMroDataPath));
			fileList.add(new InputPath(date, "Mre", mreDataPath));
			fileList.add(new InputPath(date, "HwMdtLog", huaweiMdtLogDataPath));
			fileList.add(new InputPath(date, "HwMdtImm", huaweiMdtImmDataPath));
			fileList.add(new InputPath(date, "ZteMdtLog", zteMdtLogDataPath));
			fileList.add(new InputPath(date, "ZteMdtImm", zteMdtImmDataPath));
			fileList.add(new InputPath(date, "Mme", xdrDataPath_mme));
			fileList.add(new InputPath(date, "S1uHttp", xdrDataPath_http));
			fileList.add(new InputPath(date, "Loc", xdrLocationPath));
					//添加xdr路径
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)) 
			{
				LocAllEXMain.step_down = 24;
				LocAllEXMain.step_up = 0;
			}
			ArrayList<HashMap<Integer, String>> inputPathLists = LocAllExUtil.getInputPathMaps(conf, statTime);
			for(int i=0;i<inputPathLists.size();i++)
	 		{
				HashMap<Integer, String> imsiMap=inputPathLists.get(i);
				for (Integer key : imsiMap.keySet())
				{
					TypeIoEvtEnum type = TypeIoEvtEnum.getEvtEnumByIndex(key);
					//排除 http 和 mme，mro
					if (type == TypeIoEvtEnum.ORIGIN_MRO || type == TypeIoEvtEnum.ORIGINHTTP || type == TypeIoEvtEnum.ORIGINMME)
						continue;
					String dataType = type.getFileName().replaceFirst("tbEventOrigin", "");
					String url = imsiMap.get(key);
					fileList.add(new InputPath(date, dataType, url));
				}
	 		}
			
			//写文件
			String path = mroXdrMergePath + "/MyFlag/data_" + statTime + "/tb_check_inputs_dd_" + TimeUtil.format(date, "yyMMdd") +"/INPUT_FILE_SIZE.txt";
			FileWriter.writeToFile(conf, path, new FileWriter.AbstractLineGetter<InputPath>(fileList) {

				Date date = new Date();
				@Override
				protected String toLine(InputPath f) {
					if(f.size <= 0) {
//						System.out.println(String.format("There is not files in path [%s]", f.url));
						return "";
					}
					return new StringBuilder().append(f.date.getTime()/1000).append("\t").append(f.dataType)
							.append("\t").append(f.size).append("\t").append(date.getTime()/1000).toString();
				}
			});
			hdfsOper.mkfile(mroXdrMergePath + "/MyFlag/data_" + statTime + "/tb_check_inputs_dd_" + TimeUtil.format(date, "yyMMdd") + "/_SUCCESS");//为了自动下载工具能自动下载
		}
	}

	/**
	 * 删除路径下除带sample,err,hsr,LOC_LIB,MYLOG的表以外的所有数据
	 */
	public static void deleteData(HDFSOper hdfsOper, String path) throws Exception
	{
		if(hdfsOper.checkFileExist(path))
		{
			List<FileStatus> mrolocnewResList = hdfsOper.listFlolderStatus(path);
			for (FileStatus fileStatus : mrolocnewResList)
			{
				String curPath = fileStatus.getPath().toString();
				if(curPath.toLowerCase().contains("sample") || curPath.toLowerCase().contains("err") || curPath.toLowerCase().contains("hsr") || curPath.toUpperCase().contains("LOC_LIB") || curPath.toUpperCase().contains("MYLOG") || curPath.toLowerCase().contains("mrru") || curPath.toLowerCase().contains("_hd_"))
				{
					continue;
				}
				//删除output中除_SUCCESS以外的文件
				if(curPath.toLowerCase().contains("output"))
				{
					List<FileStatus> outputResList = hdfsOper.listFileStatus(curPath, true);
					for (FileStatus file : outputResList)
					{
						String curFile = file.getPath().toString();
						if(curFile.toUpperCase().contains("_SUCCESS"))
						{
							continue;
						}
						hdfsOper.delete(curFile);
					}
					continue;
				}
				hdfsOper.delete(curPath);
			}
		}
	}

	private static boolean checkDoStat(String type)
	{
		if (statTypeMap.values().size() == 0)
		{
			return true;
		}
		return statTypeMap.containsKey(type) ? true : false;
	}

	public static boolean hwRegist()
	{
		// Init environment, load xml file
		Configuration conf = SecurityUtils.getConfiguration();
		// Security login
		if (!SecurityUtils.login(conf))
		{
			return false;
		}

		return true;
	}

	public static Date parseDate(String startTime){
		Date date = null;
		String dateStr = startTime.substring(3);
		if(dateStr.length() == 6) //18052816
		{
			date = TimeUtil.parse(dateStr, "yyMMdd");

		}else if (dateStr.length() == 8){

			date = TimeUtil.parse(dateStr, "yyMMddHH");

		}else {

			throw new IllegalArgumentException("Argument time is error! Please type in 01_yyMMdd[HH]");
		}

		return date;
	}


	private static String formatDate(final Date date, String str){

		return StringUtil.match(str, dateFormatGetter, new StringUtil.StrMatchCallBack() {
			@Override
			public String handle(String oriStr, String matchWord) {
				String dateFormat = matchWord.substring(2, matchWord.length()-1);
				return oriStr.replace(matchWord, TimeUtil.format(date, dateFormat));
			}
		});
	}

	public static String getRealFSPath(Date date, String path, Configuration conf){

		String result = "";
		if(path==null || path.length()==0){
			return result;
		}

		String pathWithDate = formatDate(date, path);
		try{

			result =  FileMatcher.matchPathStr(pathWithDate, conf, ",");

		}catch (Exception e){
			LOG.error(String.format("path not exists : %s", path));
		}

		if(result.length() > 0){
			return result;
		}else{
			return pathWithDate;
		}
	}

}
