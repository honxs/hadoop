package cn.mastercom.bigdata.spark.evtstat;
import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.mroxdrmerge.AppConfig;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import java.util.ArrayList;
import java.util.HashMap;

public class EvtStatUtil {
    public static SparkConf makeConf() throws Exception {
        Configuration conf = new Configuration();
        MainModel.GetInstance().setConf(conf);
        if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
        {
            conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
        }

        HDFSOper hdfsOper = new HDFSOper(conf);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Spark.MroXdrMerge." + "MergeStat:");


        if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
        {
            sparkConf.setMaster("local");
        }else{
//			sparkConf.set("yarn.nodemanager.resource.memory-mb", "8192");
//			sparkConf.set("yarn.scheduler.maximum-allocation-mb", "8192");
        }
        return sparkConf;
    }

    /**
     * TODO 部分地市有变化，不能用这个了
     * 通过读取 app.地市名.xml表，读取到各个各个输入文件的路径
     * 其中有的地市比较特殊，所以加了特殊的处理
     * @param startTime
     * @return 一个list  List1: imsiMap, list2: s1apidMap
     */
    public static ArrayList<HashMap<Integer, String>> getInputPathMaps(String startTime)
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

        AppConfig appConfig = MainModel.GetInstance().getAppConfig();
        String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();

        if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2))
        {
            String[] hours = new String[] { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
                    "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };
            String[] minuts = new String[] { "00", "15", "30", "45" };
            StringBuffer mme = new StringBuffer();
            StringBuffer mw = new StringBuffer();
            StringBuffer sv = new StringBuffer();
            StringBuffer rx = new StringBuffer();
            StringBuffer http = new StringBuffer();
            for (int i = 0; i < hours.length; i++)
            {
                for (int j = 0; j < minuts.length; j++)
                {
                    mme.append(appConfig.getMmePath() + "/20" + startTime.substring(3) + "/" + hours[i] + "/"
                            + minuts[j] + ",");
                    mw.append(appConfig.getMwPath() + "/20" + startTime.substring(3) + "/" + hours[i] + "/" + minuts[j]
                            + ",");
                    sv.append(appConfig.getSvPath() + "/20" + startTime.substring(3) + "/" + hours[i] + "/" + minuts[j]
                            + ",");
                    rx.append(appConfig.getRxPath() + "/20" + startTime.substring(3) + "/" + hours[i] + "/" + minuts[j]
                            + ",");
                    http.append(appConfig.getHttpPath()+"/20" + startTime.substring(3) + "/" + hours[i] + "/" + minuts[j]
                            + ",");
                }
            }
            // 转换成String,去掉最后一个字段
            mmePath = mme.toString().substring(0, mme.length() - 1);
            mwPath = mw.toString().substring(0, mw.length() - 1);
            svPath = sv.toString().substring(0, sv.length() - 1);
            rxPath = rx.toString().substring(0, rx.length() - 1);
            httpPath = http.toString().substring(0,http.length()-1);
        }
        else if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
        {
            String[] hours = new String[] {"12", "13", "14", "15", "16", "17", "18","19","20","21","22","23"};
            String[] minuts = new String[] { "00", "05", "10", "15", "20", "25", "30", "35", "40", "45", "50", "55" };
            StringBuffer mme = new StringBuffer();
            StringBuffer http = new StringBuffer();
//			StringBuffer mg = new StringBuffer();
            StringBuffer sv = new StringBuffer();
            StringBuffer mro = new StringBuffer();
            StringBuffer rtp = new StringBuffer();
            StringBuffer mw = new StringBuffer();

            for (int i = 0; i < hours.length; i++)
            {
                for (int j = 0; j < minuts.length; j++)
                {
                    String mmePath1 = "/user/wangjun/S_O_DPI_LTE_S1_MME/load_time_d=20" + startTime.substring(3)
                            + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";
                    String httpPath1 = "/user/wangjun/S_O_DPI_LTE_S1U_HTTP/load_time_d=20" + startTime.substring(3)
                            + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";
                    String mgPath1 = "/user/wangjun/S_O_DPI_VL_MG/load_time_d=20" + startTime.substring(3)
                            + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";
                    String svPath1 = "/user/wangjun/S_O_DPI_VL_SV/load_time_d=20" + startTime.substring(3)
                            + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";
                    String mroPathZTE = "/user/lixiushan/O_MR_MRO_LTE_SCPLR/load_time_d=20"+startTime.substring(3)
                            + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "/manu=ZTE";
                    String mroPathNOKIA = "/user/lixiushan/O_MR_MRO_LTE_SCPLR/load_time_d=20"+startTime.substring(3)
                            + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "/manu=NOKIA";
                    String mroPathHUAWEI = "/user/lixiushan/O_MR_MRO_LTE_SCPLR/load_time_d=20"+startTime.substring(3)
                            + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "/manu=HUAWEI";
                    String mroPathericERIC = "/user/lixiushan/O_MR_MRO_LTE_SCPLR/load_time_d=20"+startTime.substring(3)
                            + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "/manu=ERIC";

                    String rtpPath1 = "/user/wangjun/S_O_DPI_VL_RTP/load_time_d=20" + startTime.substring(3)
                            + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";

                    String mwPath1 = "/user/wangjun/S_O_DPI_VL_MW/load_time_d=20" + startTime.substring(3)
                            + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";

                    mme.append(mmePath1).append(",");
                    http.append(httpPath1).append(",");
//					mg.append(mgPath1).append(",");
                    sv.append(svPath1).append(",");
                    mro.append(mroPathZTE).append(",").append(mroPathNOKIA).append(",")
                            .append(mroPathHUAWEI).append(",").append(mroPathericERIC).append(",");
                    rtp.append(rtpPath1).append(",");
                    mw.append(mwPath1).append(",");

                }
            }



            mmePath = mme.toString().substring(0, mme.length() - 1);
            httpPath = http.toString().substring(0, http.length() - 1);
//			mgPath = mg.toString().substring(0, mg.length() - 1);
            svPath = sv.toString().substring(0, sv.length() - 1);
            rtpPath = rtp.toString().substring(0,rtp.length()-1);
            mwPath = mw.toString().substring(0, mw.length()-1);

            String mroPath = mro.toString().substring(0,mro.length()-1);

            s1apidMap.put(TypeIoEvtEnum.ORIGIN_MRO.getIndex(),mroPath);
        }
        else if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
            String[] hours = new String[] { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
                    "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };

            StringBuffer mw = new StringBuffer();
            StringBuffer sv = new StringBuffer();
            StringBuffer rx = new StringBuffer();

            for (int i = 0; i < hours.length; i++){

                String mw1 = String.format(appConfig.getMwPath(), startTime.substring(3))+"/"+hours[i];
                String sv1 = String.format(appConfig.getSvPath(), startTime.substring(3))+"/"+hours[i];
                String rx1 = String.format(appConfig.getRxPath(), startTime.substring(3))+"/"+hours[i];
                mw.append(mw1).append(",");
                sv.append(sv1).append(",");
                rx.append(rx1).append(",");

            }
            mwPath = mw.toString().substring(0, mw.length() - 1);
            svPath = sv.toString().substring(0, sv.length() - 1);
            rxPath = rx.toString().substring(0, rx.length() - 1);


        } 
        else if (MainModel.GetInstance().getCompile().Assert(CompileMark.TianJin)) 
        {
        }
        else
        {
            if(MainModel.GetInstance().getCompile().Assert(CompileMark.GanSu)) 
            {

                httpPath = String.format(appConfig.getHttpPath(),startTime.substring(3, 7) ,startTime.substring(3,9));
                mmePath = String.format(appConfig.getMmePath(),startTime.substring(3, 7) ,startTime.substring(3,9));

            }else
            {
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


}
