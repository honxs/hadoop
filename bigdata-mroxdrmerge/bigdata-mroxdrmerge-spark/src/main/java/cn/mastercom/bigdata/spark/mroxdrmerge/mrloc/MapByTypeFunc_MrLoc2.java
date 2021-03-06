package cn.mastercom.bigdata.spark.mroxdrmerge.mrloc;

import java.util.Iterator;
import java.util.List;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.spark.MapByTypeFuncBaseV2;
import cn.mastercom.bigdata.util.spark.TypeInfo;
import cn.mastercom.bigdata.mro.loc.CellTimeKey;
import cn.mastercom.bigdata.mro.loc.MroXdrDeal;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroBsTablesEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsFgTableEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsOTTTableEnum;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.mdt.stat.MdtTablesEnum;
import scala.Tuple2;
import scala.Tuple3;

public class MapByTypeFunc_MrLoc2 extends MapByTypeFuncBaseV2 implements org.apache.spark.api.java.function.PairFlatMapFunction<Iterator<Tuple2<String, Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>>>, TypeInfo, Iterable<String>>
{
	private static final long serialVersionUID = 1L;

	private ResultOutputer resultOutputer;
	private MroXdrDeal mroXdrDeal;
	
	public MapByTypeFunc_MrLoc2(Object[] args) throws Exception
	{
		super(args);
		registerTableInfo();
	}

    @Override
    protected int init_once_sub() throws Exception
	{
		// 初始化基础配置
		Configuration conf = new Configuration();
		
		//XDRLOCALL事件
		conf.set("mapreduce.job.date", dateStr);
		MainModel.GetInstance().setConf(conf);
		
		resultOutputer = new ResultOutputer(dataOutputMng);
		mroXdrDeal = new MroXdrDeal(resultOutputer);
		
		// 打印状态日志
		LOGHelper.GetLogger().writeLog(LogType.info,"ltecell init count is : " + CellConfig.GetInstance().getLteCellInfoMap().size());

		return 0;
	}

    public void registerTableInfo()
    {
		for (MroBsTablesEnum mroBsTablesEnum : MroBsTablesEnum.values())
		{
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.ChongQing))
			{
				if(!mroBsTablesEnum.getDirName().contains("user"))
				{
					TypeInfo typeInfo = new TypeInfo(mroBsTablesEnum.getIndex(), mroBsTablesEnum.getFileName(), mroBsTablesEnum.getHourPath(mroXdrMergePath, dateStr, hourStr));
					registTypeInfo(typeInfo);
				}
			}
			else
			{
				TypeInfo typeInfo = new TypeInfo(mroBsTablesEnum.getIndex(), mroBsTablesEnum.getFileName(), mroBsTablesEnum.getHourPath(mroXdrMergePath, dateStr, hourStr));
				registTypeInfo(typeInfo);
			}
		}
		
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NoCsTable))
		{
			if(!MainModel.GetInstance().getCompile().Assert(CompileMark.ChongQing))
			{
				registTypeInfo(new TypeInfo(MroCsOTTTableEnum.mrocell.getIndex(), MroCsOTTTableEnum.mrocell.getFileName(), MroCsOTTTableEnum.mrocell.getHourPath(mroXdrMergePath, dateStr, hourStr)));
				registTypeInfo(new TypeInfo(MroCsOTTTableEnum.mrloclib.getIndex(), MroCsOTTTableEnum.mrloclib.getFileName(), MroCsOTTTableEnum.mrloclib.getHourPath(mroXdrMergePath, dateStr, hourStr)));
				registTypeInfo(new TypeInfo(MroCsOTTTableEnum.xdrloclib.getIndex(), MroCsOTTTableEnum.xdrloclib.getFileName(), MroCsOTTTableEnum.xdrloclib.getHourPath(mroXdrMergePath, dateStr, hourStr)));
				registTypeInfo(new TypeInfo(MroCsFgTableEnum.indoorErr.getIndex(), MroCsFgTableEnum.indoorErr.getFileName(), MroCsFgTableEnum.indoorErr.getHourPath(mroXdrMergePath, dateStr, hourStr)));
				registTypeInfo(new TypeInfo(MroCsFgTableEnum.AOATA_PATH.getIndex(), MroCsFgTableEnum.AOATA_PATH.getFileName(), MroCsFgTableEnum.AOATA_PATH.getHourPath(mroXdrMergePath, dateStr, hourStr)));
			}
		}
		else
		{
			for (MroCsOTTTableEnum mroCsOTTTableEnum : MroCsOTTTableEnum.values())
			{
				TypeInfo typeInfo = new TypeInfo(mroCsOTTTableEnum.getIndex(), mroCsOTTTableEnum.getFileName(), mroCsOTTTableEnum.getHourPath(mroXdrMergePath, dateStr, hourStr));
				registTypeInfo(typeInfo);
			}
			
			for (MroCsFgTableEnum mroCsFgTableEnum : MroCsFgTableEnum.values())
			{
				TypeInfo typeInfo = new TypeInfo(mroCsFgTableEnum.getIndex(), mroCsFgTableEnum.getFileName(), mroCsFgTableEnum.getHourPath(mroXdrMergePath, dateStr, hourStr));
				registTypeInfo(typeInfo);
			}
		}
		/**
		 * mro过程事件表格的注册
		 */
		for(TypeIoEvtEnum typeSampleEvtIO : TypeIoEvtEnum.values())
		{	
			String mroPath = MroCsOTTTableEnum.getBasePath(mroXdrMergePath, dateStr);
//			LOGHelper.GetLogger().writeLog(LogType.info, "mroPath的输出路径为 : " + mroPath);
//			LOGHelper.GetLogger().writeLog(LogType.info, "事件的输出路径为 : " + typeSampleEvtIO.getHourPath(mroPath, "01_" + dateStr, hourStr));
//			System.out.println("mroPath的输出路径为 : " + mroPath + "   事件的输出路径为 : " + typeSampleEvtIO.getHourPath(mroPath, "01_" + dateStr, hourStr));
			TypeInfo typeInfo = new TypeInfo(typeSampleEvtIO.getIndex(), "mro" + typeSampleEvtIO.getFileName(), typeSampleEvtIO.getHourPath(mroPath, "01_" + dateStr, hourStr));
			registTypeInfo(typeInfo);
		}
		
		for (MdtTablesEnum mdtTablesEnum : MdtTablesEnum.values())
		{
			TypeInfo typeInfo = new TypeInfo(mdtTablesEnum.getIndex(), mdtTablesEnum.getFileName(), mdtTablesEnum.getHourPath(mroXdrMergePath, dateStr, hourStr));
			registTypeInfo(typeInfo);
		}
    }

	private void registTypeInfo(TypeInfo typeInfo)
	{
		if (!MainModel.GetInstance().getCompile().Assert(CompileMark.SaveToHive))
		{
			typeInfoMng_HDFS.registTypeInfo(typeInfo);
		}
		else
		{
			typeInfoMng_RDD.registTypeInfo(typeInfo);
		}
	}
    
    public int init_eciKey(String eci_time)
    {
    	CellTimeKey cellKey = new CellTimeKey();
    	String[] arrays = eci_time.split("_",-1);
    	cellKey.setEci(Long.parseLong(arrays[0]));
    	cellKey.setTimeSpan(Integer.parseInt(arrays[1]));
    	mroXdrDeal.init(cellKey);
    	return 0;
    }
    
	@Override
	public Iterable<Tuple2<TypeInfo, Iterable<String>>> call(Iterator<Tuple2<String, Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>>> str) throws Exception {
		// TODO Auto-generated method stub
		// 初始化配置
		init_once();
		while(str.hasNext())
		{
			Tuple2<String,Tuple3<Iterable<String>,Iterable<String>, Iterable<String>>> tp = str.next();
			String eciTime = tp._1;
			Tuple3<Iterable<String>, Iterable<String>, Iterable<String>> tp3 = tp._2;
			init_eciKey(eciTime);
			Iterator<String> it2 = tp3._2().iterator();
			while(it2.hasNext())
			{
				try
				{
					String xdrLocData = it2.next();
					mroXdrDeal.pushData(MroXdrDeal.DataType_XDRLOCATION, xdrLocData);
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeDetailLog(LogType.error,"xdr pushData Error", "xdr pushData Error: " , e);
				}
				
			}
			
			Iterator<String> it3 = tp3._3().iterator();
			while(it3.hasNext())
			{
				try
				{
					String cellBuildData = it3.next();
					mroXdrDeal.pushData(MroXdrDeal.DataType_CELLBUILD, cellBuildData);
				} 
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeDetailLog(LogType.error,"cellBuild pushData Error", "cellBuild pushData Error: " , e);
				}
			}
			
			Iterator<String> it1 = tp3._1().iterator();
			while(it1.hasNext())
			{
				try 
				{
					String mtMroData = it1.next();
					mroXdrDeal.pushData(MroXdrDeal.DataType_MRO_MT, mtMroData);
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error,"mtmro pushData Error", "mtmro pushData Error: " , e);
				}
			}
			mroXdrDeal.statData();
			mroXdrDeal.outData();
		}
		mroXdrDeal.dataStater.outResult();// event
		mroXdrDeal.outAllData();
		
		List<Tuple2<TypeInfo, Iterable<String>>> resultRdd = typeResult.GetRdds();
//		clear(true);
		dataOutputMng.clear();
//		typeResult.clear();
		return resultRdd;
	}
}
