package com.chinamobile.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a Constants class which holds the common default variables of storm-hbase project.
 * These variables can be overrided by storm.properties file or hbase.properties file.
 */

@NotProguard
public interface Constants
{
    //所有配置文件所在的目录
    public static final String CONFIG_FILE_PATH = "../conf/";

    public static final String DEBUG_LOG_FILE_PATHNAME = "";

    //采集部分
    //与configuration.xml中的dataSource ID保持一致
    public static final String FTPDataBase_DataSource_ID = "mysql";
    public static final String Quartz_Scheduler_Master_IP = "172.16.140.141";

    public static final String FTP_SERVER_NAME_1 = "172.16.140.40";
    public static final String FTP_SERVER_NAME_2 = "172.16.140.40";
    public static final String FTP_SERVER_NAME_3 = "172.16.140.40";

    public static final String tempZipPath1 = "/dfs01/tempZipPath/";
    public static final String tempZipPath2 = "/dfs04/tempZipPath/";               //"/dfs03/tempZipPath/"

    public static final String DFSRootLocalPath1 = "/dfs05/downfiles/";        //"/dfs02/downfiles/"
    public static final String DFSRootLocalPath2 = "/dfs04/downfiles/";        //"/dfs04/downfiles/"

    public static final String ZipFileTempPath = "/dfs04/parsefile/temp/";   //"/dfs02/parsefile/temp/"

    public static final String ParseFilePath = "/dfs04/parsefile/";       ///dfs04/parsefile/

    public static final String FTPRootLocalPath = "/data/tempdown/";


    public static final String REDIS_IP_ADDRESS = "172.16.140.132";

    public static final String REDIS_PORT = "6379";

    public static final String FLUME_SPOOL_DIR = "/dfs03/spooldir/";

    //待调整，目前NRMPM实际用于发送PM
    public static final String KAFKA_TOPIC_STRING_NRMPM = "ltepm";            //pmnrmlte

    public static final String KAFKA_TOPIC_STRING_NRM = "ltenrm";

    public static final String KAFKA_TOPIC_STRING_MRS = "mymrs";

    public static final String KAFKA_TOPIC_STRING_MRO = "mymro";

    public static final String KAFKA_TOPIC_STRING_MRE = "mymre";

    public static final String ZOOKEEPERS = "inspurdell04:2181,inspurdell05:2181,inspurdell06:2181";  //

    public static final String ZOOKEEPER_ROOT = "/usr/local/apache-storm-0.9.4/workdir"; // default zookeeper root configuration for storm

    public static final String STORM_PROP_CONF_FILE = "storm_prop_path";

    public static final String HBASE_PROP_CONF_FILE_HBASE_SITE = "/etc/hbase/conf/hbase-site.xml";

    public static final String HBASE_PROP_CONF_FILE_HDFS_SITE = "/etc/hbase/conf/hdfs-site.xml";

    public static final String HBASE_PROP_CONF_FILE_CORE_SITE = "/etc/hbase/conf/core-site.xml";

    public static final int STORM_SPOUT_DEFAULT_QUEUE_SIZE = 1000;

    public static final int STORM_SPOUT_DEFAULT_START_TIMESTAMP = 0;

    public static final int STORM_SPOUT_DEFAULT_STOP_TIMESTAMP = 0;

    public static final short HBASE_DEFAULT_SHARDING_NUM = 32;

    public static final String HBASfE_DEFAULT_TABLE_NAME = "storm-hbase";

    public static final String HBASE_DEFAULT_COLUMN_FAMILY = "cf";

    public static final String HBASE_DEFAULT_COLUMN = "c";

    public static final int HBASE_DEFAULT_CACHING_RECORD = 100;

    public static final int HBASE_STREAM_DATA_START_SEC = 3 * 60;

    public static final int HBASE_STREAM_DATA_STOP_SEC = 0;

    public static final String datFilePath = "/dfs03/datFile/";


    //MR原始文件保存目录,待修改为dfs06
    public static final String MR_RAW_FILE_DIRECTORY = "/dfs03/mro/";

    //MR采集文件元信息保存目录,待修改为dfs06
    public static final String MR_FILE_METAINFO_DIRECTORY = "/dfs03/mroinfo/";

    //河南服务器配置

    public static final String REDIS_IP_ADDRESS_HENAN115 = "127.0.0.1";
    public static final int REDIS_PORT_HENAN115 = 6379;
    public static final String XDR_QUEUE_KEY = "XDR";

    /**
     * ********添加codis的配置信息***********
     */
    public static final String[] CODIS_PROXY_LIST = {
            "172.16.140.101",
            "172.16.140.102",
            "172.16.140.103",
            "172.16.140.104"
    };
    public static final int[] CODIS_PORT_LIST = {
            19000,
            19002
    };
    public static final String CODIS_READ_IP_ADDRESS = "172.16.140.104";
    public static final String CODIS_WRITE_IP_ADDRESS = "172.16.140.102";
    public static final String CODIS_PORT = "19000";
    /**
     * 备用测试地址*
     */
//    public static final String CODIS_READ_IP_ADDRESS="172.16.140.132";
//    public static final String CODIS_WRITE_IP_ADDRESS="172.16.140.132";
//    public static final String CODIS_PORT = "6379";

    public static final String JITUAN_FTP_SERVER_IP = "10.1.122.68";
    public static final String JITUAN_FTP_SERVER_PORT = "21";
    public static final String JITUAN_FTP_SERVER_USERNAME = "jt";
    public static final String JITUAN_FTP_SERVER_PASSWORD = "wuxianyouhua";
    //FTP服务器上的目录结构
    public static final String[] JITUAN_FTP_PROVINCE_LIST = {"anhui", "beijing", "chongqing", "fujian", "gansu", "guangdong"
            , "guangxi", "guizhou", "hainan", "hebei", "heilongjiang", "henan", "hubei", "hunan"
            , "jiangsu", "jiangxi", "jilin", "liaoning", "neimenggu", "ningxia", "qinghai"
            , "shandong", "shanghai", "shanxi", "shx", "sichuan", "tianjin", "yunnan", "zhejiang" };
    public static final String[] JITUAN_FTP_PROVINCE_LIST2 = {"zhejiang" };
    public static final String LTE_RESOURCE_FILE_PATH = "/dfs03/networkresource";

    //网格编号使用
    public static final double lngMin = 73.33;
    public static final double lngMax = 135.05;
    public static final double latMin = 3.51;
    public static final double latMax = 53.33;
    public static final int M = 10000;
    public static final int N = 10000;

    //工参
    public static final String REDIS_PREFIX_WIRELESS_RESOURCE_CGI = "WR";
    //分省统计小区总数
    public static final String REDIS_PREFIX_WRTOTALP = "WRTOTALP";
    //分城市、场景、室内外、频段统计工参总数
    public static final String REDIS_PREFIX_WIRELESS_RESOURCE_TOTAL = "WRTOTAL";
    //网格编号
    public static final String REDIS_PREFIX_WIRELESS_RESOURCE_GRIDID = "LTESG";
    //采集NRMPM文件的元信息
    public static final String REDIS_PREFIX_DOWNLOAD_FILE_METAINFO = "DOWN";
    //待解析的NRMPM文件队列
    public static final String REDIS_KEY_TO_PARSE_NRMPM_QUEUE = "ToParse";
    //待解析的MR文件队列
    public static final String REDIS_KEY_TO_PARSE_MR_QUEUE = "MRToParse";
    //MRO统计每个小区的覆盖率、重叠覆盖
    public static final String REDIS_PREFIX_MRO_COVERAGE = "MRO";
    //MRO邻区出现次数排序
    public static final String REDIS_PREFIX_MRO_DATA_CHECK = "DC";
    //主服小区大于-110的样本总数
    public static final String REDIS_PREFIX_MRO_NETWORK_STRUCTURE = "NS";
    //主服小区大于-110的强邻区样本排序
    public static final String REDIS_PREFIX_MRO_STRONG_NEIGHBOR_RANK = "RK";
    //PM中四个切换性能统计
    public static final String REDIS_PREFIX_PM_HANDOVER = "HO";
    //ECI集合，用于全量分析
    public static final String REDIS_PREFIX_NRM_ECISET = "ECISet";
    //列模式前缀
    public static final String REDIS_PREFIX_COLUMNMODE = "ColumnMode";
    //已解析各厂家版本的集合
    public static final String REDIS_KEY_PARSED = "Parsed";
    //EnbOfDn
    public static final String REDIS_PREFIX_NRM_EnbOfDn = "EnbOfDn";
    //ECIOfDn
    public static final String REDIS_PREFIX_NRM_ECIOfDn = "ECIOfDn";
    //DnOfECI
    public static final String REDIS_PREFIX_NRM_DnOfECI = "DnOfECI";
    //Cause集合
    public static final String REDIS_CAUSE_SET = "CauseSet";
    //原因值索引最大值
    public static final String REDIS_KEY_CAUSE_ALIAS_MAX_INDEX = "CauseAliasMaxIndex";
    //原因值索引最大值
    public static final String REDIS_PREFIX_CAUSE_ALIAS = "CauseAlias";
    //KPI指标前缀
    public static final String REDIS_PREFIX_KPI = "KPI";
    //Counter集合
    public static final String REDIS_COUNTER_SET = "CounterSet";
    //NRM Counter集合
    public static final String REDIS_NRMCOUNTER_SET = "NRMCounterSet";
    //PM Counter集合
    public static final String REDIS_PMCOUNTER_SET = "PMCounterSet";
    //CauseSet集合
    public static final String REDIS_CAUSESET = "CauseSet";
    //KPI集合
    public static final String REDIS_KPI_SET = "KPISet";
    //KPI门限规则过滤出问题小区
    public static final String REDIS_KPI_RULESET = "KPIRuleSet";
    //KPI门限规则
    public static final String REDIS_PREFIX_KPI_RULE = "KPIRule";
    //存放满足门限条件的小区
    public static final String REDIS_PREFIX_KPI_RULE_RANK = "KRR";

    //原因值索引最大值
    public static final String REDIS_PREFIX_KPI_ALIAS = "Alias";
    //邻区距离
    public static final String REDIS_PREFIX_NEIGHBOR_DISTANCE = "ND";
    //基站级的经纬度计算
    public static final String REDIS_PREFIX_MRO_PARA_CHECK = "PC";
    //全国指标汇总（用于大汇总）
    public static final String REDIS_PREFIX_PM_SUMMARY = "SM";
    //汇总每个省的NRM DN集合
    public static final String REDIS_PREFIX_NRM_DNSET = "NRMDNSET";
    //汇总每个省的PM DN集合
    public static final String REDIS_PREFIX_PM_DNSET = "PMDNSET";
    //counter汇总方式
    public static final String REDIS_PREFIX_COUNTER_SUM_METHOD = "SUM";

    //counter汇总方式
    public static final String REDIS_PREFIX_HourSummaryStatus = "HourSummaryStatus";
    //counter汇总方式
    public static final String REDIS_SET_HourSummaryFinish = "HourSummaryFinish";

    //MR弱覆盖
    public static final String MR_WEAK_COVERAGE = "WC";

    public static final String[] PROVINCE_CODE_LIST = {"AH", "BJ", "CQ", "FJ", "GD", "GS", "GX", "GZ", "HA", "HB", "HE", "HI",
            "HL", "HN", "JL", "JS", "JX", "LN", "NM", "NX", "QH", "SC", "SD", "SH", "SN", "SX", "TJ", "XJ", "XZ", "YN", "ZJ" };

    public static final Map<String, String> PROVINCENAME_MAP = new HashMap<String, String>()
    {{
            put("-1", "EE");
            //中文省份缩写
            put("安徽", "AH");
            put("北京", "BJ");
            put("重庆", "CQ");
            put("福建", "FJ");
            put("广东", "GD");
            put("甘肃", "GS");
            put("广西", "GX");
            put("贵州", "GZ");
            put("河南", "HA");
            put("湖北", "HB");
            put("河北", "HE");
            put("海南", "HI");
            put("黑龙江", "HL");
            put("湖南", "HN");
            put("吉林", "JL");
            put("江苏", "JS");
            put("江西", "JX");
            put("辽宁", "LN");
            put("内蒙", "NM");
            put("内蒙古", "NM");
            put("宁夏", "NX");
            put("青海", "QH");
            put("四川", "SC");
            put("山东", "SD");
            put("上海", "SH");
            put("陕西", "SN");
            put("山西", "SX");
            put("天津", "TJ");
            put("新疆", "XJ");
            put("西藏", "XZ");
            put("云南", "YN");
            put("浙江", "ZJ");

            //中文省份全称
            put("安徽省", "AH");
            put("北京市", "BJ");
            put("重庆市", "CQ");
            put("福建省", "FJ");
            put("广东省", "GD");
            put("甘肃省", "GS");
            put("广西省", "GX");
            put("广西壮族自治区", "GX");
            put("贵州省", "GZ");
            put("河南省", "HA");
            put("湖北省", "HB");
            put("河北省", "HE");
            put("海南省", "HI");
            put("黑龙江省", "HL");
            put("湖南省", "HN");
            put("吉林省", "JL");
            put("江苏省", "JS");
            put("江西省", "JX");
            put("辽宁省", "LN");
            put("内蒙古省", "NM");
            put("内蒙古自治区", "NM");
            put("宁夏省", "NX");
            put("宁夏回族自治区", "NX");
            put("青海省", "QH");
            put("四川省", "SC");
            put("山东省", "SD");
            put("上海市", "SH");
            put("陕西省", "SN");
            put("山西省", "SX");
            put("天津市", "TJ");
            put("新疆省", "XJ");
            put("新疆维吾尔族自治区", "XJ");
            put("西藏省", "XZ");
            put("西藏自治区", "XZ");
            put("云南省", "YN");
            put("浙江省", "ZJ");

            //拼音省份缩写
            put("anhui", "AH");
            put("beijing", "BJ");
            put("chongqing", "CQ");
            put("fujian", "FJ");
            put("guangdong", "GD");
            put("gansu", "GS");
            put("guangxi", "GX");
            put("guizhou", "GZ");
            put("henan", "HA");
            put("hubei", "HB");
            put("hebei", "HE");
            put("hainan", "HI");
            put("heilongjiang", "HL");
            put("hunan", "HN");
            put("jilin", "JL");
            put("jiangsu", "JS");
            put("jiangxi", "JX");
            put("liaoning", "LN");
            put("neimenggu", "NM");
            put("ningxia", "NX");
            put("qinghai", "QH");
            put("sichuan", "SC");
            put("shandong", "SD");
            put("shanghai", "SH");
            put("shanxi", "SX");
            put("shaanxi", "SN");
            put("shx", "SN");
            put("tianjin", "TJ");
            put("xinjiang", "XJ");
            put("xizang", "XZ");
            put("yunnan", "YN");
            put("zhejiang", "ZJ");

            //拼音省份缩写
            put("AH", "AH");
            put("BJ", "BJ");
            put("CQ", "CQ");
            put("FJ", "FJ");
            put("GD", "GD");
            put("GS", "GS");
            put("GX", "GX");
            put("GZ", "GZ");
            put("HA", "HA");
            put("HB", "HB");
            put("HE", "HE");
            put("HI", "HI");
            put("HL", "HL");
            put("HN", "HN");
            put("JL", "JL");
            put("JS", "JS");
            put("JX", "JX");
            put("LN", "LN");
            put("NM", "NM");
            put("NX", "NX");
            put("QH", "QH");
            put("SC", "SC");
            put("SD", "SD");
            put("SH", "SH");
            put("SX", "SX");
            put("SN", "SN");
            put("TJ", "TJ");
            put("XJ", "XJ");
            put("XZ", "XZ");
            put("YN", "YN");
            put("ZJ", "ZJ");
        }};

    public final static Map<String, String> PROVINCECODE_E2C = new HashMap<String, String>()
    {{
            put("EE", "其他");
            //中文省份缩写
            put("AH", "安徽");
            put("BJ", "北京");
            put("CQ", "重庆");
            put("FJ", "福建");
            put("GD", "广东");
            put("GS", "甘肃");
            put("GX", "广西");
            put("GZ", "贵州");
            put("HA", "河南");
            put("HB", "湖北");
            put("HE", "河北");
            put("HI", "海南");
            put("HL", "黑龙江");
            put("HN", "湖南");
            put("JL", "吉林");
            put("JS", "江苏");
            put("JX", "江西");
            put("LN", "辽宁");
            put("NM", "内蒙");
            put("NM", "内蒙古");
            put("NX", "宁夏");
            put("QH", "青海");
            put("SC", "四川");
            put("SD", "山东");
            put("SH", "上海");
            put("SN", "陕西");
            put("SX", "山西");
            put("TJ", "天津");
            put("XJ", "新疆");
            put("XZ", "西藏");
            put("YN", "云南");
            put("ZJ", "浙江");
        }};

    public final static Map<String, String> VENDORNAME_C2E = new HashMap<String, String>()
    {{
            put("中兴", "ZTE");
            put("爱立信", "Ericsson");
            put("大唐", "Datang");
            put("诺基亚", "NSN");
            put("诺西", "NSN");
            put("NOKIA", "NSN");
            put("阿尔卡特", "ALU");
            put("上海贝尔阿尔卡特", "ALU");
            put("上海贝尔", "ALU");
            put("阿朗", "ALU");
            put("卡特", "ALU");
            put("贝尔", "ALU");
            put("sbell", "ALU");
            put("SBell", "ALU");
            put("新邮通", "Postcom");
            put("普天", "Putian");
            put("华为", "Huawei");

        }};

    public final static Map<String, String> VENDORNAME_E2C = new HashMap<String, String>()
    {{
            put("ZTE", "中兴");
            put("Ericsson", "爱立信");
            put("Datang", "大唐");
            put("NSN", "诺西");
            put("Huawei", "华为");
            put("ALU", "贝尔");
            put("Postcom", "新邮通");
            put("Putian", "普天");
        }};

    public final static String[] VENDOR_LIST = {"ZTE", "Ericsson", "Datang", "NSN", "Huawei", "ALU", "Postcom", "Putian" };


    public final static Map<String, String> COVERTYPE_C2E = new HashMap<String, String>()
    {{
            put("1", "indoor");
            put("室内", "indoor");
            put("室分", "indoor");
            put("室内站", "indoor");
            put("室分站", "indoor");

            put("0", "outdoor");
            put("室外", "outdoor");
            put("室外站", "outdoor");
            put("室内外", "outdoor");
            put("室内外站", "outdoor");
            put("室内室外", "outdoor");
            put("室内室外站", "outdoor");
        }};

    public final static Map<String, String> COVERTYPE_E2C = new HashMap<String, String>()
    {{
            put("indoor", "室内");
            put("outdoor", "室外");
        }};

    public final static String[] COVERTYPE_LIST = {"indoor", "outdoor" };


    public final static String[] northbaseIPList = {
            "172.16.140.15",
            "172.16.140.16",
            "172.16.140.18",
            "172.16.140.19",
            "172.16.140.20",
            "172.16.140.21",
            "172.16.140.30",
            "172.16.140.40",
            "172.16.140.101",
            "172.16.140.102",
            "172.16.140.103",
            "172.16.140.104",
            "172.16.140.105",
            "172.16.140.106",
            "172.16.140.107",
            "172.16.140.108",
            "172.16.140.109",
            "172.16.140.110",
            "172.16.140.111",
            "172.16.140.130",
            "172.16.140.131",
            "172.16.140.132",
            "172.16.140.133",
            "172.16.140.134",
            "172.16.140.135",
            "172.16.140.141"
            //"10.10.4.34",
            //"10.10.4.35",
            //"10.10.4.36"
    };


    public final static Map<String, String> SERVER_LOCATION = new HashMap<String, String>()
    {{
            put("172.16.140.15", "H9_15");
            put("172.16.140.16", "H9_16");
            put("172.16.140.18", "H10_18");
            put("172.16.140.19", "H10_17");
            put("172.16.140.20", "I2_192");
            put("172.16.140.21", "H12_19");
            put("172.16.140.30", "H12_20");
            put("172.16.140.40", "C1_49");
            put("172.16.140.101", "I2_191");
            put("172.16.140.102", "I2_190");
            put("172.16.140.103", "I2_189");
            put("172.16.140.104", "I2_188");
            put("172.16.140.105", "D12_136");
            put("172.16.140.106", "C1_52");
            put("172.16.140.107", "C1_51");
            put("172.16.140.108", "I3_195");
            put("172.16.140.109", "I3_196");
            put("172.16.140.110", "I3_197");
            put("172.16.140.111", "I3_198");
            put("172.16.140.130", "I1_181");
            put("172.16.140.131", "I1_182");
            put("172.16.140.132", "I1_183");
            put("172.16.140.133", "I1_184");
            put("172.16.140.134", "I1_185");
            put("172.16.140.135", "I1_186");
            put("172.16.140.141", "C1_50");

            //put("10.10.4.34","H7_12");
            //put("10.10.4.35","H8_14");
            //put("10.10.4.36","H8_13");

        }};


    public final static String[] northbaseProcessList = {
            "sss",
            "tomcat",
            "NorthBaseMonitor",
            "QuartzScheduler",
            "FileDownloadTest",
            "ParseDataTest"
    };


    public static final ArrayList<String[]> ReplaceString_Map = new ArrayList<String[]>()
    {
        {
            add(new String[]{"北向", "" });
            add(new String[]{"报告", "" });
            add(new String[]{"数据", "" });
            add(new String[]{"资源", "CM" });
            add(new String[]{"配置", "CM" });
            add(new String[]{"参数", "CM" });
            add(new String[]{"性能", "PM" });
            add(new String[]{"测量", "MR" });
            add(new String[]{"上海贝尔", "ALU" });
            add(new String[]{"中兴", "ZTE" });
            add(new String[]{"贝尔", "ALU" });
            add(new String[]{"爱立信", "Ericsson" });
            add(new String[]{"大唐", "Datang" });
            add(new String[]{"诺基亚", "NSN" });
            add(new String[]{"诺西", "NSN" });
            add(new String[]{"华为", "Huawei" });
            add(new String[]{"阿朗", "ALU" });
            add(new String[]{"卡特", "ALU" });
            add(new String[]{"新邮通", "Postcom" });
            add(new String[]{"NOKIA", "NSN" });
            add(new String[]{"普天", "Putian" });
            add(new String[]{"贝尔", "ALU" });
            add(new String[]{"省", "" });
            add(new String[]{"安徽", "anhui" });
            add(new String[]{"北京", "beijing" });
            add(new String[]{"福建", "fujian" });
            add(new String[]{"甘肃", "gansu" });
            add(new String[]{"广东", "guangdong" });
            add(new String[]{"广西", "guangxi" });
            add(new String[]{"贵州", "guizhou" });
            add(new String[]{"海南", "hainan" });
            add(new String[]{"河北", "hebei" });
            add(new String[]{"河南", "henan" });
            add(new String[]{"黑龙江", "heilongjiang" });
            add(new String[]{"湖北", "hubei" });
            add(new String[]{"湖南", "hunan" });
            add(new String[]{"吉林", "jilin" });
            add(new String[]{"江苏", "jiangsu" });
            add(new String[]{"江西", "jiangxi" });
            add(new String[]{"辽宁", "liaoning" });
            add(new String[]{"内蒙古", "neimenggu" });
            add(new String[]{"宁夏", "ningxia" });
            add(new String[]{"青海", "qinghai" });
            add(new String[]{"山东", "shandong" });
            add(new String[]{"山西", "shanxi" });
            add(new String[]{"陕西", "shaanxi" });
            add(new String[]{"上海", "shanghai" });
            add(new String[]{"四川", "sichuan" });
            add(new String[]{"天津", "tianjin" });
            add(new String[]{"西藏", "xizang" });
            add(new String[]{"新疆", "xinjiang" });
            add(new String[]{"云南", "yunnan" });
            add(new String[]{"浙江", "zhejiang" });
            add(new String[]{"重庆", "chongqing" });
        }
    };

}