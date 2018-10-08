package cn.mastercom.bigdata.mapr.evt.locall;

import cn.mastercom.bigdata.evt.locall.model.*;
import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.evt.locall.stat.XdrLocallexDeal2;
import cn.mastercom.bigdata.mapr.Main;
import cn.mastercom.bigdata.mapr.evt.locall.LocAllMapper_Imsi.ResidentUserMapper;
import cn.mastercom.bigdata.mapr.evt.locall.LocAllMapper_Imsi.UserLocMapper_XDRLOC;
import cn.mastercom.bigdata.mapr.evt.locall.LocAllMapper_Imsi.XdrDataMapper_Imsi;
import cn.mastercom.bigdata.mapr.evt.locall.LocAllMapper_S1apidEci.SiapidUserLocMapper_XDRLOC;
import cn.mastercom.bigdata.mapr.evt.locall.LocAllMapper_S1apidEci.XdrDataMapper_S1apid;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.mroxdrmerge.AppConfig;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileMatcher;
import cn.mastercom.bigdata.util.hadoop.mapred.CombineSmallFileInputFormat;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class LocAllExUtil
{

	/**
	 * MultipleInputs来添加输入路径，并且判断是否有imsi的数据或者s1apid的数据
	 * 这个调用了LocAllEXMain中的变量 haveImsiData, haveS1apidData
	 * @param job 一个mapreduce的job
	 * @param imsiOrS1apidMap 这个要么是s1apid的map， 要么是imsi的map
	 * @param imsiOrS1apid 这个要么是字符串s1apid, 要么是字符串imsi
	 * @author zhaikaishun
	 */
	public static void addInputPath(Job job, HashMap<Integer, String> imsiOrS1apidMap, String imsiOrS1apid)
	{
		boolean haveData = false;

		
		for (Map.Entry<Integer, String> dataTypePath : imsiOrS1apidMap.entrySet())
		{
			String eventpath = dataTypePath.getValue();
			String[] paths = eventpath.split(",", -1);
			for (String evtPath : paths)
			{
				if (evtPath.length() > 0 && LocAllEXMain.hdfsOper.checkDirExist(evtPath))
				{
					if ("imsi".equals(imsiOrS1apid))
					{
						MultipleInputs.addInputPath(job, new Path(evtPath), CombineSmallFileInputFormat.class,
								XdrDataMapper_Imsi.class);
						haveData = true;
						LocAllEXMain.haveImsiData=true;
					}
					else
					{
						MultipleInputs.addInputPath(job, new Path(evtPath), CombineSmallFileInputFormat.class,
								XdrDataMapper_S1apid.class);
						haveData = true;
						LocAllEXMain.haveS1apidData = true;
					}

					System.err.println("find xdr path: " + evtPath);
				}
			}
		}
		
		if (LocAllEXMain.hdfsOper != null && LocAllEXMain.hdfsOper.checkDirExist(LocAllEXMain.xdr_locPath))
		{
			if(haveData){
				if ("imsi".equals(imsiOrS1apid))
				{
					MultipleInputs.addInputPath(job, new Path(LocAllEXMain.xdr_locPath), CombineTextInputFormat.class,
							UserLocMapper_XDRLOC.class);
				}
				else
				{
					MultipleInputs.addInputPath(job, new Path(LocAllEXMain.xdr_locPath), CombineTextInputFormat.class,
							SiapidUserLocMapper_XDRLOC.class);
				}
				System.err.println("find loc path: " + LocAllEXMain.xdr_locPath);
			}
			
			
		}


		if (LocAllEXMain.hdfsOper != null && LocAllEXMain.hdfsOper.checkDirExist(LocAllEXMain.mr_locPath))
		{
			if(haveData){
				if ("imsi".equals(imsiOrS1apid))
				{
					MultipleInputs.addInputPath(job, new Path(LocAllEXMain.mr_locPath), CombineTextInputFormat.class,
							UserLocMapper_XDRLOC.class);
				}
				else
				{
					MultipleInputs.addInputPath(job, new Path(LocAllEXMain.mr_locPath), CombineTextInputFormat.class,
							SiapidUserLocMapper_XDRLOC.class);
				}

				System.err.println("find loc path: " + LocAllEXMain.mr_locPath);
			}

		}

		/* 增加常驻用户 */
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.ResidentUser)){
			if(LocAllEXMain.hdfsOper != null && LocAllEXMain.resident_userPath.length()>0
					&& LocAllEXMain.hdfsOper.checkDirExist(LocAllEXMain.resident_userPath))
			{
				MultipleInputs.addInputPath(job, new Path(LocAllEXMain.resident_userPath), CombineSmallFileInputFormat.class,
						ResidentUserMapper.class);
				System.err.println("find  Resident Path: " + LocAllEXMain.resident_userPath);
			}
		}

	}

	/**
	 * @param imsiOrS1apidMap
	 * @return reducenum数
	 * @throws Exception
	 */
	public static int getReduceNum(HashMap<Integer, String> imsiOrS1apidMap) throws Exception
	{
		long inputSize = 0;
		int reduceNum = 1;
		if (LocAllEXMain.hdfsOper.checkDirExist(LocAllEXMain.xdr_locPath))
		{
			inputSize+= MapReduceMainTools.getFileSize(LocAllEXMain.xdr_locPath, LocAllEXMain.hdfsOper);
		}
		if (LocAllEXMain.hdfsOper.checkDirExist(LocAllEXMain.mr_locPath))
		{
			inputSize+= MapReduceMainTools.getFileSize(LocAllEXMain.mr_locPath, LocAllEXMain.hdfsOper);
		}

		for (Map.Entry<Integer, String> dataTypePath : imsiOrS1apidMap.entrySet())
		{
			String[] evnet_inpaths = dataTypePath.getValue().split(",");
			for (int i = 0; i < evnet_inpaths.length; ++i)
			{
				if (evnet_inpaths[i].length() > 0 && LocAllEXMain.hdfsOper.checkDirExist(evnet_inpaths[i]))
				{
					inputSize+= MapReduceMainTools.getFileSize(evnet_inpaths[i], LocAllEXMain.hdfsOper);
				}
			}
		}
			
		if (inputSize > 0)
		{
			int dealReduceSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getDealSizeReduce());
			double sizeG = inputSize * 1.0 / (1024 * 1024 * 1024);
			double sizePerReduce = dealReduceSize / 1024.0;
			reduceNum = Math.max((int) (sizeG / sizePerReduce), reduceNum);

			LocAllEXMain_ImsiOrS1apid.LOG.info("total input size of data is : " + sizeG + " G ");
			LocAllEXMain_ImsiOrS1apid.LOG.info("the count of reduce to go is " + reduceNum);
		}
		return reduceNum;
	}

	/**
	 * 调用 MultiOutputMngV2 来给输出设置别名，在reduce处吐出的时候，按照别名吐出到对应的目录
	 * @param job
	 * @param outpath 日志的输入路径
	 * @param loc_all XDRLOCALL的目录，一般就是"/xdr_locall"
	 * @param imsiOrS1apid "imsi" 或者 "s1apid"
	 */
	public static void addNamedOutput(Job job, String outpath, String loc_all, String imsiOrS1apid)
	{
		
		for (TypeIoEvtEnum typeSampleEvtIO : TypeIoEvtEnum.values())
		{

			String curOutPath = "";
			String[] hours = new String[] { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
					"12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };
			//内蒙的输出有点特殊
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
			{

                if (typeSampleEvtIO.getOutPath().startsWith("/TB_EVENT")) {
                    curOutPath = typeSampleEvtIO.getPath(LocAllEXMain.mroXdrMergePath + loc_all + "/data_01_20"
                                    + LocAllEXMain.startTime.substring(3),
                            "00_01_20" + LocAllEXMain.startTime.substring(3));

                    curOutPath = curOutPath.replace("TB_EVENT_ORIGIN_S1U_HTTP_", "TB_EVENT_ORIGIN_S1HTTP_");
                    curOutPath = curOutPath.replace("TB_EVENT_ORIGIN_MG_", "TB_EVENT_ORIGIN_S1MG_");
                    curOutPath = curOutPath.replace("TB_EVENT_ORIGIN_MME_", "TB_EVENT_ORIGIN_S1MME_");
                    curOutPath = curOutPath.replace("TB_EVENT_ORIGIN_SV_", "TB_EVENT_ORIGIN_S1NEIMENGSV_");
                    curOutPath = curOutPath.replace("TB_EVENT_ORIGIN_RTP_", "TB_EVENT_ORIGIN_S1RTP_");
                }else{
                    curOutPath = typeSampleEvtIO.getPath(LocAllEXMain.mroXdrMergePath +
                            loc_all + "/data_01_20" + LocAllEXMain.startTime.substring(3)
                            + "/" + imsiOrS1apid, LocAllEXMain.startTime);
                }


				
				if(imsiOrS1apid.toUpperCase().equals("S1APID")){
					if(!curOutPath.contains("TB_EVENT_ORIGIN_MRO_LTE_SCPLR"))
					{
						continue;
					}
				}		
			}else if(MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
				curOutPath = typeSampleEvtIO.getPath(LocAllEXMain.mroXdrMergePath +
						loc_all + "/data_" + LocAllEXMain.startTime
						+"/"+hours[LocAllEXMain.step_up]+ "/" + imsiOrS1apid, LocAllEXMain.startTime);
			}
			else
			{
				curOutPath = typeSampleEvtIO.getPath(LocAllEXMain.mroXdrMergePath + 
						loc_all + "/data_" + LocAllEXMain.startTime
						+ "/" + imsiOrS1apid, LocAllEXMain.startTime);
			}
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
				MultiOutputMngV2.addNamedOutput(job, typeSampleEvtIO.getIndex(), imsiOrS1apid+LocAllEXMain
								.step_up+typeSampleEvtIO
								.getFileName(),
						curOutPath, TextOutputFormat.class, NullWritable.class, Text.class);
			}else{
				MultiOutputMngV2.addNamedOutput(job, typeSampleEvtIO.getIndex(), imsiOrS1apid+typeSampleEvtIO.getFileName(),
						curOutPath, TextOutputFormat.class, NullWritable.class, Text.class);
			}



		}
		FileOutputFormat.setOutputPath(job, new Path(outpath));
	}

	/**
	 * 通过读取 app.地市名.xml表，读取到原始数据的路径
	 * 其中有的地市比较特殊，所以加了特殊的处理
	 * @param conf
	 * @param startTime 类似于01_180125
	 * @return 一个list  第一个元素: imsiMap, 第二个元素: s1apidMap. Map结构（key：数据ID，value: 数据路径）
	 */
	public static ArrayList<HashMap<Integer, String>> getInputPathMaps(Configuration conf, String startTime)
	{

		ArrayList<HashMap<Integer, String>> imsiAndS1apidLists = new ArrayList<HashMap<Integer, String>>();
		HashMap<Integer, String> imsiMap = new HashMap<>();
		HashMap<Integer, String> s1apidMap = new HashMap<>();
		imsiAndS1apidLists.add(imsiMap);
		imsiAndS1apidLists.add(s1apidMap);
		String mmePath = "";
		String mwPath = "";
		String svPath = "";
		String rxPath = "";
		String httpPath = "";
		String mgPath = "";
		String rtpPath = "";
		String uuPath = "";
		String mosPath = "";
		String dhwjtPath = "";
		String imsMoPath = "";
		String imsMtPath = "";
		String qualityPath = "";
		String mdtPath="";
		String mdtPath2="";
		String mdtPathRcef="";
		String mosShardingPath="";
		String mdtImm1Path = "";
        String mdtImm4Path = "";
        String mdtImm5Path = "";
        String mdtHofRlfRcefPath = "";

		AppConfig appConfig = MainModel.GetInstance().getAppConfig();
		String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();

		/*广西分小时目录，特殊处理*/
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2))
		{
			Date date = Main.parseDate(startTime);
			mwPath = Main.getRealFSPath(date, appConfig.getMwPath(), conf);
			svPath = Main.getRealFSPath(date, appConfig.getSvPath(), conf);
			rxPath = Main.getRealFSPath(date, appConfig.getRxPath(), conf);
			mmePath = Main.getRealFSPath(date, appConfig.getMmePath(), conf);
			httpPath = Main.getRealFSPath(date, appConfig.getHttpPath(), conf);

		}
		/*内蒙特殊处理，TODO 20180701 改成使用通配符的形式*/
		else if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			Date date = Main.parseDate(startTime);
			mmePath = Main.getRealFSPath(date, appConfig.getMmePath(), conf);
			httpPath = Main.getRealFSPath(date, appConfig.getHttpPath(), conf);
			svPath = Main.getRealFSPath(date, appConfig.getSvPath(), conf);
			rtpPath = Main.getRealFSPath(date, appConfig.getRtpPath(), conf);
			mwPath = Main.getRealFSPath(date, appConfig.getMwPath(), conf);
			rxPath = Main.getRealFSPath(date, appConfig.getRxPath(), conf);
			if(!appConfig.getMTMroDataPath().equals("/mapr/")){
				String mtMroDataPath = appConfig.getMTMroDataPath();
				mtMroDataPath = mtMroDataPath.replace("O_MR_MRO_LTE_NCELL","O_MR_MRO_LTE_SCPLR");
				String mroPath=Main.getRealFSPath(date, mtMroDataPath, conf);
                s1apidMap.put(TypeIoEvtEnum.ORIGIN_MRO.getIndex(),mroPath);
            }
		}
		/*四川特殊处理，TODO 20180701 改成使用通配符的形式*/
		else if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
			String[] hours = new String[] { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
					"12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };
			String curRegexp = "";
			for (int i = LocAllEXMain.step_up; i <LocAllEXMain.step_down ; i++) {
				curRegexp+=hours[i]+",";
			}
			curRegexp = "{"+curRegexp.substring(0,curRegexp.length()-1)+"}";
			Date date = Main.parseDate(startTime);
			httpPath = Main.getRealFSPath(date, appConfig.getHttpPath()+curRegexp, conf);
			mwPath = Main.getRealFSPath(date, appConfig.getMwPath()+curRegexp, conf);
			svPath = Main.getRealFSPath(date, appConfig.getSvPath()+curRegexp, conf);
			rxPath = Main.getRealFSPath(date, appConfig.getRxPath()+curRegexp, conf);
			mdtHofRlfRcefPath = Main.getRealFSPath(date, appConfig.getMdtHofRlfRcefPath()+curRegexp, conf);
			
		} else if (MainModel.GetInstance().getCompile().Assert(CompileMark.TianJin)){
			Date date = Main.parseDate(startTime);
			// /user/hive/warehouse/stg.db/o_se_ur_volte_sv_tdr/day=20180623/hour=00
			// /user/hive/warehouse/stg.db/o_se_ur_volte_rx_tdr/day=20180623/hour=00
			// /user/hive/warehouse/stg.db/o_se_ur_volte_mw_tdr/day=20180623/hour=00
			mwPath = Main.getRealFSPath(date, appConfig.getMwPath(), conf);
			svPath = Main.getRealFSPath(date, appConfig.getSvPath(), conf);
			rxPath = Main.getRealFSPath(date, appConfig.getRxPath(), conf);
			httpPath = Main.getRealFSPath(date, appConfig.getHttpPath(), conf);
			qualityPath = Main.getRealFSPath(date, appConfig.getQuaLityPath(), conf);
			mmePath = Main.getRealFSPath(date,appConfig.getMmePath(),conf);
		}
		else
		{
			Date date = Main.parseDate(startTime);
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.GanSu)){
                httpPath = String.format(appConfig.getHttpPath(),startTime.substring(3, 7) ,startTime.substring(3,9));
                mmePath = String.format(appConfig.getMmePath(),startTime.substring(3, 7) ,startTime.substring(3,9));

            }else{
				httpPath = String.format(appConfig.getHttpPath(), startTime.substring(3));
				mmePath = String.format(appConfig.getMmePath(), startTime.substring(3));
			}
			
			mwPath = String.format(appConfig.getMwPath(), startTime.substring(3));
			svPath = String.format(appConfig.getSvPath(), startTime.substring(3));
			rxPath = String.format(appConfig.getRxPath(), startTime.substring(3));
			uuPath = String.format(appConfig.getUuPath(), startTime.substring(3));
			mosPath = String.format(appConfig.getMosPath(), mroXdrMergePath, startTime.substring(3));
			dhwjtPath = String.format(appConfig.getDhwjtPath(), mroXdrMergePath, startTime.substring(3));
			imsMoPath = String.format(appConfig.getImsMoPath(), startTime.substring(3));
			imsMtPath = String.format(appConfig.getImsMtPath(), startTime.substring(3));
			qualityPath = String.format(appConfig.getQuaLityPath(), startTime.substring(3));
			mdtPath = String.format(appConfig.getMdtPath(), startTime.substring(3));
			mdtPath2 = String.format(appConfig.getMdtPath2(), startTime.substring(3));
			mdtPathRcef = String.format(appConfig.getMdtPath3(), startTime.substring(3));
			mosShardingPath =String.format(appConfig.getMosShardingPath(), startTime.substring(3));
		    //IMM几种数据
            mdtImm1Path = String.format(appConfig.getMdtImm1(), startTime.substring(3));
            mdtImm4Path = String.format(appConfig.getMdtImm4(), startTime.substring(3));
            mdtImm5Path = String.format(appConfig.getMdtImm5(), startTime.substring(3));
			mdtHofRlfRcefPath = Main.getRealFSPath(date,appConfig.getMdtHofRlfRcefPath(),conf);
		}

		if("IMSI".equals(appConfig.getMdtHofRlfRcefFillType().toUpperCase())){
			imsiMap.put(TypeIoEvtEnum.ORIGIN_MDT_RLFHOFRCEF.getIndex(), mdtHofRlfRcefPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGIN_MDT_RLFHOFRCEF.getIndex(), mdtHofRlfRcefPath);
		}

		// set data_type
		if ("IMSI".equals(appConfig.getMmeFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGINMME.getIndex(), mmePath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGINMME.getIndex(), mmePath);
		}

		if ("IMSI".equals(appConfig.getMwFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGINMW.getIndex(), mwPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGINMW.getIndex(), mwPath);
		}

		if ("IMSI".equals(appConfig.getSvFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGINSV.getIndex(), svPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGINSV.getIndex(), svPath);
		}

		if ("IMSI".equals(appConfig.getRxFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGINRX.getIndex(), rxPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGINRX.getIndex(), rxPath);
		}

		if ("IMSI".equals(appConfig.getHttpFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGINHTTP.getIndex(), httpPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGINHTTP.getIndex(), httpPath);
		}

		if ("IMSI".equals(appConfig.getMgFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGINMG.getIndex(), mgPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGINMG.getIndex(), mgPath);
		}

		if ("IMSI".equals(appConfig.getRtpFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGINRTP.getIndex(), rtpPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGINRTP.getIndex(), rtpPath);
		}

		if ("IMSI".equals(appConfig.getUuFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGIN_Uu.getIndex(), uuPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGIN_Uu.getIndex(), uuPath);
		}

		if ("IMSI".equals(appConfig.getMosFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGINMOS_BEIJING.getIndex(), mosPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGINMOS_BEIJING.getIndex(), mosPath);
		}

		if ("IMSI".equals(appConfig.getDhwjtFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGINWJTDH_BEIJING.getIndex(), dhwjtPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGINWJTDH_BEIJING.getIndex(), dhwjtPath);
		}

		if ("IMSI".equals(appConfig.getImsMoFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGINIMS_MO.getIndex(), imsMoPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGINIMS_MO.getIndex(), imsMoPath);
		}

		if ("IMSI".equals(appConfig.getImsMtFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGINIMS_MT.getIndex(), imsMtPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGINIMS_MT.getIndex(), imsMtPath);
		}

		if ("IMSI".equals(appConfig.getQuaLityFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGINCDR_QUALITY.getIndex(), qualityPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGINCDR_QUALITY.getIndex(), qualityPath);
		}
		if ("IMSI".equals(appConfig.getMdtFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGIN_MDT_RLFHOF.getIndex(), mdtPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGIN_MDT_RLFHOF.getIndex(), mdtPath);
		}
		if ("IMSI".equals(appConfig.getMdtFillType2().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGIN_MDT_RLFHOF_OTHERPATH.getIndex(), mdtPath2);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGIN_MDT_RLFHOF_OTHERPATH.getIndex(), mdtPath2);
		}
		//mdtPathRcef
		if ("IMSI".equals(appConfig.getMdtRcefFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGIN_MDT_RCEF.getIndex(), mdtPathRcef);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGIN_MDT_RCEF.getIndex(), mdtPathRcef);
		}


		
		//mosShardingPath
		if ("IMSI".equals(appConfig.getMosShardingFillType().toUpperCase()))
		{
			imsiMap.put(TypeIoEvtEnum.ORIGIN_MOS_SHARDING.getIndex(), mosShardingPath);
		}
		else
		{
			s1apidMap.put(TypeIoEvtEnum.ORIGIN_MOS_SHARDING.getIndex(), mosShardingPath);
		}
		/*mdt的三种数据  */
        if ("IMSI".equals(appConfig.getMdtImm1FillType().toUpperCase()))
        {
            imsiMap.put(TypeIoEvtEnum.ORIGIN_MDT_IMM1.getIndex(), mdtImm1Path);
        }
        else
        {
            s1apidMap.put(TypeIoEvtEnum.ORIGIN_MDT_IMM1.getIndex(), mdtImm1Path);
        }

        if ("IMSI".equals(appConfig.getMdtImm4FillType().toUpperCase()))
        {
            imsiMap.put(TypeIoEvtEnum.ORIGIN_MDT_IMM4.getIndex(), mdtImm4Path);
        }
        else
        {
            s1apidMap.put(TypeIoEvtEnum.ORIGIN_MDT_IMM4.getIndex(), mdtImm4Path);
        }

        if ("IMSI".equals(appConfig.getMdtImm5FillType().toUpperCase()))
        {
            imsiMap.put(TypeIoEvtEnum.ORIGIN_MDT_IMM5.getIndex(), mdtImm5Path);
        }
        else
        {
            s1apidMap.put(TypeIoEvtEnum.ORIGIN_MDT_IMM5.getIndex(), mdtImm5Path);
        }


		
		return imsiAndS1apidLists;
	}

	public static StringBuffer getTypeToPath(HashMap<Integer, String> imsiMap)
	{
		StringBuffer sBuffer = new StringBuffer();
		for (Map.Entry<Integer, String> dataTypePath : imsiMap.entrySet())
		{
			String[] evnet_inpaths = dataTypePath.getValue().split(",");
			for (int i = 0; i < evnet_inpaths.length; ++i)
			{
				if (evnet_inpaths[i].length() > 0 && LocAllEXMain.hdfsOper.checkDirExist(evnet_inpaths[i]))
				{
					sBuffer.append(dataTypePath.getKey() + "#" + evnet_inpaths[i] + ";");
				}
			}
		}
		return sBuffer;
	}

	/**
	 * 内蒙输出原始数据，特殊地区
	 * @param xdrLocallexDeal
	 * @param dataType
	 * @param value
	 */
	public static void neimengPushCPEData(XdrLocallexDeal2 xdrLocallexDeal, int dataType, String value) {
		try {
			XdrDataBase xdrDataObject = XdrDataFactory.
					GetInstance().getXdrDataObject(dataType);
			StringBuffer sb = new StringBuffer();
			if(dataType==TypeIoEvtEnum.ORIGINMME.getIndex()){
				XdrData_Mme xdrDataMme= (XdrData_Mme)xdrDataObject;
				xdrDataMme.value.append(value);
				xdrDataMme.toString(sb);
				
			}else if(dataType==TypeIoEvtEnum.ORIGINHTTP.getIndex()){
				XdrData_Http xdrDataHttp = (XdrData_Http) xdrDataObject;
				xdrDataHttp.value.append(value);
				xdrDataHttp.toString(sb);

			}else if(dataType==TypeIoEvtEnum.ORIGINSV.getIndex()){
				XdrData_Sv XdrData_Sv = (XdrData_Sv) xdrDataObject;
				XdrData_Sv.value.append(value);
				XdrData_Sv.toString(sb);
			}else if(dataType==TypeIoEvtEnum.ORIGINRTP.getIndex()){
				XdrData_Rtp xdrData_Rtp = (XdrData_Rtp) xdrDataObject;
				xdrData_Rtp.value.append(value);
				xdrData_Rtp.toString(sb);
			}else if(dataType==TypeIoEvtEnum.ORIGINMW.getIndex()){
				XdrData_Mw xdrData_Mw = (XdrData_Mw) xdrDataObject;
				xdrData_Mw.value.append(value);
				xdrData_Mw.toString(sb);
			}else if(dataType==TypeIoEvtEnum.ORIGIN_MRO.getIndex()){
				MroData_ul_lostRackRateL mroData = (MroData_ul_lostRackRateL)xdrDataObject;
				mroData.value.append(value);
				mroData.toString(sb);
			} else if(dataType==TypeIoEvtEnum.ORIGIN_MRO.getIndex()){
				MroData_ul_lostRackRateL mroData = (MroData_ul_lostRackRateL)xdrDataObject;
				mroData.value.append(value);
				mroData.toString(sb);
			}
			xdrLocallexDeal.resultOutputer.pushData(dataType, sb.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
     }
}
