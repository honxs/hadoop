package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.text.SimpleDateFormat;
import java.util.Date;

import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.spark.RDDLog;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


public class MergeHoursMain
{
	public static final int DataType_STATE_GRID = 1;
	
		
	protected static final Log LOG = LogFactory.getLog(MroXdrMergeMain.class);

	public static String queueName;
	
	public static String inpath_data;
	
	public static String inpath_mro;
	public static String inpath_mre;
	
	public static String outpath;
	public static String outpath_table;
	public static String outpath_date;
	
	private static RDDLog rddLog = new RDDLog();
	private static HDFSOper hdfsOper;
	private static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmm");

	private static void makeConfig(Configuration conf, String[] args)
	{
	    queueName = args[0];
	    outpath_date = args[1];
	    inpath_data = args[2];
	    outpath_table = args[3];
	    
	    for(int i=0; i<args.length; ++i)
	    {
	    	LOG.info(i + ": " + args[i] + "\n");
	    }
	   
		String fsurl = "hdfs://" + MainModel.GetInstance().getAppConfig().getHadoopHost() + ":" + MainModel.GetInstance().getAppConfig().getHadoopHdfsPort();
		conf.set("fs.defaultFS", fsurl);
		
		MainModel.GetInstance().setConf(conf);
	}	
	
	public static void DoSparkJob(String[] args) throws Exception 
	{	
	    if (args.length != 13) 
		{
		    System.err.println("Usage: xdr lable fill <in-mro> <in-xdr> <out path> <out table path> <out path date>");
		    System.exit(2);
	    }
	    
	    //make config
	    Configuration conf = new Configuration();   
	    makeConfig(conf, args);	
	    
	    //do work		
		Date stime = new Date();
		
		LOGHelper.GetLogger().addWriteLogCallBack(rddLog);
		
		hdfsOper = new HDFSOper(conf);
		
		String tarPath = "";
        hdfsOper.reNameExistsPath(outpath_table, tarPath);
		
		CreateJobSpark(conf, args);
		
		Date etime = new Date();
		int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
		String timeFileName = String.format("%s/%dMins_%s_%s", outpath_table, mins, timeFormat.format(stime),
				timeFormat.format(etime));
		hdfsOper.mkfile(timeFileName);
	}
	
	@SuppressWarnings("serial")
	public static void CreateJobSpark(final Configuration conf, final String[] args) throws Exception 
	{			     
		SparkConf sparkConf  = new SparkConf();
		sparkConf.setAppName("Spark.MroXdrMerge" + ":" + outpath_date);
		sparkConf.set("spark.driver.maxResultSize", MainModel.GetInstance().getSparkConfig().get_spark_driver_maxResultSize());
		sparkConf.set("yarn.nodemanager.resource.memory-mb",  MainModel.GetInstance().getSparkConfig().get_yarn_nodemanager_resource_memory());
	    sparkConf.set("yarn.scheduler.maximum-allocation-mb", MainModel.GetInstance().getSparkConfig().get_yarn_scheduler_maximum());
		
	    //打印日志
	    sparkConf.set("yarn.log-aggregation-enable", "true");    
		
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			sparkConf.setMaster("local");
		}
		
	    //do work
	    final JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    Object[] params;

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	    //xdr loc
//	    String path_xdrloc = outpath_table + "/xdr_loc";
//	    JavaRDD<String> mmeRdd = ctx.textFile(inpath_xdr_mme).repartition(20);
//	    JavaRDD<String> httpRdd = ctx.textFile(inpath_xdr_http).repartition(20);
//	    JavaRDD<String> locationRdd = ctx.textFile(inpath_location).repartition(20);
//	    
//	    MapByImsiFunc_MME mapByImsiFunc_MME = new MapByImsiFunc_MME();
//	    JavaPairRDD<Long, String> pairMmeRdd = mmeRdd.flatMapToPair(mapByImsiFunc_MME);
//	    
//	    MapByImsiFunc_HTTP mapByImsiFunc_HTTP = new MapByImsiFunc_HTTP();
//	    JavaPairRDD<Long, String> pairHttpRdd = httpRdd.flatMapToPair(mapByImsiFunc_HTTP);	
//	    
//	    MapByImsiFunc_Location mapByImsiFunc_Location = new MapByImsiFunc_Location();
//	    JavaPairRDD<Long, String> locRdd = locationRdd.flatMapToPair(mapByImsiFunc_Location);
//	    
//	    params = new Object[2];
//	    params[0] = outpath_date;
//	    params[1] = path_xdrloc;
//	    MapByTypeFunc_XdrLoc mapByTypeFunc_XdrDeal = new MapByTypeFunc_XdrLoc(params);
//	    JavaPairRDD<TypeInfo, Iterable<String>> xdrLocRDD = pairMmeRdd.cogroup(pairHttpRdd).repartition(20).flatMapToPair(mapByTypeFunc_XdrDeal);
//	    xdrLocRDD.persist(StorageLevel.MEMORY_AND_DISK());
//	    
//	    RDDTypeResult xdrDealTypeResult = new  RDDTypeResult(mapByTypeFunc_XdrDeal.getTypeInfoMng(), xdrLocRDD);
//
//        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//	    //xdr location
//	    String path_xdrLocation = outpath_table + "/xdr_location";
//        JavaRDD<String> xdrLocationRDD = xdrDealTypeResult.getResult(Arrays.asList(MapByTypeFunc_XdrLoc.DataType_XdrLocation));
//        FilterTimeFunc_XdrLocation filterTimeFunc_XdrLocation = new FilterTimeFunc_XdrLocation(600);
//        JavaRDD<String> xdrLocationRDD_LastSpan = xdrLocationRDD.filter(filterTimeFunc_XdrLocation);
//	    	    
//        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//	    //mr loc
//	    String path_mrloc = outpath_table + "/mr_loc";
//	    JavaRDD<String> mroRdd = ctx.textFile(inpath_mro);
//	    JavaRDD<String> mreRdd = ctx.textFile(inpath_mre);
//	    
//	    JavaRDD<String> xdrLocationStrRDD = xdrDealTypeResult.getResult(Arrays.asList(MapByTypeFunc_XdrLoc.DataType_XdrLocation));
//	    
//	    MapPartitionFunc_MrFormat mapPartitionFunc_MrFormat = new MapPartitionFunc_MrFormat();
//	    JavaRDD<String> mrFormatStrRDD = mroRdd.mapPartitions(mapPartitionFunc_MrFormat);
//	        
//	    MapByCellFunc_Mro mapByCellFunc_Mro = new MapByCellFunc_Mro();
//	    JavaPairRDD<String, String> eci_mroRdd = mrFormatStrRDD.flatMapToPair(mapByCellFunc_Mro).repartition(100);
//	    
//	    MapByCellFunc_XdrLocation mapByCellFunc_XdrLocation = new MapByCellFunc_XdrLocation();
//	    JavaPairRDD<String, String> eci_xdrLocationRdd = xdrLocationStrRDD.flatMapToPair(mapByCellFunc_XdrLocation);	    
//	    
//	    params = new Object[2];
//	    params[0] = outpath_date;
//	    params[1] = path_mrloc;
//	    MapByTypeFunc_MrLoc mapByTypeFunc_MrLoc = new MapByTypeFunc_MrLoc(params);
//	    JavaPairRDD<TypeInfo, Iterable<String>> mrlocRDD = eci_mroRdd.cogroup(eci_xdrLocationRdd).repartition(20).flatMapToPair(mapByTypeFunc_MrLoc);
//
//	    RDDTypeResult mrLocTypeResult = new  RDDTypeResult(mapByTypeFunc_MrLoc.getTypeInfoMng(), mrlocRDD);
//	    
//        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//	    //grid stat
//	    String path_gridstat = outpath_table + "/gridstat";
//	     
//	    JavaPairRDD<TypeInfo, Iterable<String>> mroCellGridStrRDD = mrLocTypeResult.getPairResult(Arrays.asList(
//	    		MapByTypeFunc_MrLoc.DataType_CellGridStat_4G,
//	    		MapByTypeFunc_MrLoc.DataType_DT_CellGridStat_4G,
//	    		MapByTypeFunc_MrLoc.DataType_CQT_CellGridStat_4G,
//	    		MapByTypeFunc_MrLoc.DataType_CellGridStat10_4G,
//	    		MapByTypeFunc_MrLoc.DataType_DT_CellGridStat10_4G,
//	    		MapByTypeFunc_MrLoc.DataType_CQT_CellGridStat10_4G,
//	    		MapByTypeFunc_MrLoc.DataType_DT_CellGridStat_Freq,
//	    		MapByTypeFunc_MrLoc.DataType_CQT_CellGridStat_Freq,
//	    		MapByTypeFunc_MrLoc.DataType_DT_CellGridStat10_Freq,
//	    		MapByTypeFunc_MrLoc.DataType_CQT_CellGridStat10_Freq));
//	    
//	    MapByGridFunc_MroGrid mapByGridFunc_MroGrid = new MapByGridFunc_MroGrid();
//	    JavaPairRDD<GridTypeItem, Iterable<String>> mroGridStatRDD = mroCellGridStrRDD.flatMapToPair(mapByGridFunc_MroGrid);
//	    
//	    JavaPairRDD<GridTypeItem, Iterable<Iterable<String>>> gridUnionRDD = mroGridStatRDD.groupByKey();
//	   
//	    params = new Object[2];
//	    params[0] = outpath_date;
//	    params[1] = path_gridstat;
//	    MapByTypeFunc_GridStat mapByTypeFunc_GridStat = new MapByTypeFunc_GridStat(params);
//	    JavaPairRDD<TypeInfo, Iterable<String>> gridStatRDD = gridUnionRDD.flatMapToPair(mapByTypeFunc_GridStat);
//	    
//	    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//	    //save act
//	    RDDResultOutputer rddResultOutputer = new RDDResultOutputer(mapByTypeFunc_XdrDeal.getTypeInfoMng(), xdrLocRDD);
//	    JobConf jobConf = new JobConf(conf);
//	    rddResultOutputer.saveAsHadoopFile(path_xdrloc, jobConf, 
//	    		Arrays.asList(MapByTypeFunc_XdrLoc.DataType_DTEvent, 
//             		   MapByTypeFunc_XdrLoc.DataType_CQTEvent,
//             		   MapByTypeFunc_XdrLoc.DataType_DTEXEvent,
//             		   MapByTypeFunc_XdrLoc.DataType_UserStat,
//             		   MapByTypeFunc_XdrLoc.DataType_UserActCell,
//             		   MapByTypeFunc_XdrLoc.DataType_XdrLocation));
//	    
//	    xdrLocationRDD_LastSpan.saveAsTextFile(path_xdrLocation);
//	    
//	    rddResultOutputer = new RDDResultOutputer(mapByTypeFunc_MrLoc.getTypeInfoMng(), mrlocRDD);
//	    rddResultOutputer.saveAsHadoopFile(path_mrloc, jobConf, 
//	    		Arrays.asList(MapByTypeFunc_MrLoc.DataType_DT_Sample, 
//                		MapByTypeFunc_MrLoc.DataType_CQT_Sample,
//                		MapByTypeFunc_MrLoc.DataType_DTEX_Sample,
//                		MapByTypeFunc_MrLoc.DataType_All_Sample,
//                		MapByTypeFunc_MrLoc.DataType_UserStat,
//                		MapByTypeFunc_MrLoc.DataType_CellStat_4G,
//                		MapByTypeFunc_MrLoc.DataType_CellGridStat_4G,
//                		MapByTypeFunc_MrLoc.DataType_CellStat_Freq,
//                		MapByTypeFunc_MrLoc.DataType_DT_CellGridStat_4G,
//                		MapByTypeFunc_MrLoc.DataType_CQT_CellGridStat_4G,
//                		MapByTypeFunc_MrLoc.DataType_MR_OutGridCell));
//	    
//	    rddResultOutputer = new RDDResultOutputer(mapByTypeFunc_GridStat.getTypeInfoMng(), gridStatRDD);
//	    rddResultOutputer.saveAsHadoopFile(path_gridstat, jobConf, 
//	    		Arrays.asList(MapByTypeFunc_GridStat.DataType_DTGrid, 
//	    				MapByTypeFunc_GridStat.DataType_CQTGrid,
//	    				MapByTypeFunc_GridStat.DataType_Grid,
//	    				MapByTypeFunc_GridStat.DataType_DTGrid_10,
//	    				MapByTypeFunc_GridStat.DataType_CQTGrid_10,
//	    				MapByTypeFunc_GridStat.DataType_Grid_10,
//	    				MapByTypeFunc_GridStat.DataType_DTGrid_Freq,
//	    				MapByTypeFunc_GridStat.DataType_CQTGrid_Freq,
//	    				MapByTypeFunc_GridStat.DataType_DTGrid_Freq_10,
//	    				MapByTypeFunc_GridStat.DataType_CQTGrid_Freq_10));
//	   
//	    ctx.stop();
	}
	
	public static void main(String[] args) throws Exception 
	{ 			
        DoSparkJob(args);
    }
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
//
//	public static void main(String[] args)
//	{
//		String[] paras = new String[] { DataType_STATE_GRID + "", "state_grid",
//				"hdfs://192.168.1.31:9000/filtertest/out0921",
//				"hdfs://192.168.1.31:9000/mt_wlyh/Data/mroxdrmerge/mro_loc/data_01_170221/TB_DTSIGNAL_GRID_01_170221",
//				"spark.mroxdrmerge.gridstat.GridMergeDo_4G" };
//		MergeHoursMain mergeHoursMain = new MergeHoursMain();
//		mergeHoursMain.startTask(paras);
//	}
//
//	public void startTask(String[] paras)
//	{
//		MergeMain mergeMain = new MergeMain();
//		TypeInfoMng typeInfoMng = new TypeInfoMng();
//		// TODO
//		int type = Integer.parseInt(paras[0]);
//		String typeName = paras[1];
//		String typePath = paras[2];
//		String srcPath = paras[3];
//		String mergeClassName = paras[4];
//		// TypeInfo typeInfo = new
//		// MergeTypeInfo(DataType_STATE_GRID,"state_grid","hdfs://192.168.1.31:9000/filtertest/out0921",
//		// "hdfs://192.168.1.31:9000/mt_wlyh/Data/mroxdrmerge/mro_loc/data_01_170221/TB_DTSIGNAL_GRID_01_170221","spark.mroxdrmerge.gridstat.GridMergeDo_4G");
//		TypeInfo typeInfo = new MergeTypeInfo(type, typeName, typePath, srcPath, mergeClassName);
//
//		typeInfoMng.registTypeInfo(typeInfo);
//		Configuration conf = new Configuration();
//		// String fsurl = "hdfs://" +
//		// MainModel.GetInstance().getAppConfig().getHadoopHost() + ":" +
//		// MainModel.GetInstance().getAppConfig().getHadoopHdfsPort();
//		String fsurl = "hdfs://192.168.1.31:9000";
//		conf.set("fs.defaultFS", fsurl);
//
//		Object[] objects = new Object[] { typeInfoMng, typeInfo.getTypePath() };
//		try
//		{
//			mergeMain.mergeData(conf, objects);
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//	}

}
