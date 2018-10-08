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
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import scala.Tuple2;

public class MapByTypeFunc_MrLoc extends MapByTypeFuncBaseV2 implements org.apache.spark.api.java.function.PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>, TypeInfo, Iterable<String>>
{
	private static final long serialVersionUID = 1L;

	private ResultOutputer resultOutputer;
	private MroXdrDeal mroXdrDeal;
	
	public MapByTypeFunc_MrLoc(Object[] args) throws Exception
	{
		super(args);
		registerTableInfo();
	}

    @Override
    protected int init_once_sub() throws Exception
	{
		// 初始化基础配置
		Configuration conf = new Configuration();
		MainModel.GetInstance().setConf(conf);
		
		resultOutputer = new ResultOutputer(dataOutputMng);
		mroXdrDeal = new MroXdrDeal(resultOutputer);
		return 0;
	}

    public void registerTableInfo()
    {
		for (MroBsTablesEnum mroBsTablesEnum : MroBsTablesEnum.values())
		{
			TypeInfo typeInfo = new TypeInfo(mroBsTablesEnum.getIndex(), mroBsTablesEnum.getFileName(), mroBsTablesEnum.getPath(outPath, dateStr));
			typeInfoMng_HDFS.registTypeInfo(typeInfo);
		}
		
		for (MroCsFgTableEnum mroCsFgTableEnum : MroCsFgTableEnum.values())
		{
			TypeInfo typeInfo = new TypeInfo(mroCsFgTableEnum.getIndex(), mroCsFgTableEnum.getFileName(), mroCsFgTableEnum.getPath(outPath, dateStr));
			typeInfoMng_HDFS.registTypeInfo(typeInfo);
		}
		
		for (MroCsOTTTableEnum mroCsOTTTableEnum : MroCsOTTTableEnum.values())
		{
			TypeInfo typeInfo = new TypeInfo(mroCsOTTTableEnum.getIndex(), mroCsOTTTableEnum.getFileName(), mroCsOTTTableEnum.getPath(outPath, dateStr));
			typeInfoMng_HDFS.registTypeInfo(typeInfo);
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
	// 要处理的数据<eci+time,<mrolist>,<xdrlocation>>
	public Iterable<Tuple2<TypeInfo, Iterable<String>>> call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> tp) throws Exception
	{
		// 初始化配置
		init_once();
		init_eciKey(tp._1);
		try
		{
			Iterator<String> it2 = tp._2._2.iterator();
			while(it2.hasNext())
			{
				String xdrLocData = it2.next();
				mroXdrDeal.pushData(MroXdrDeal.DataType_XDRLOCATION, xdrLocData);
			}
			Iterator<String> it1 = tp._2._1.iterator();
			while(it1.hasNext())
			{
				String mtMroData = it1.next();
				mroXdrDeal.pushData(MroXdrDeal.DataType_MRO_MT, mtMroData);
			}
			
			mroXdrDeal.statData();
//			mroXdrDeal.outData();
			mroXdrDeal.outAllData();
			
//			UserMrLoc userMrLoc = new UserMrLoc(typeResult);
//			userMrLoc.deal(tp._1, tp._2._1, tp._2._2, null);
//			userMrLoc.outResult();
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"MapByTypeFunc_MrLoc Error", "MapByTypeFunc_MrLoc Error " + e.getMessage(), e);
		}

		List<Tuple2<TypeInfo, Iterable<String>>> resultRdd = typeResult.GetRdds();
		typeResult.clear();
		return resultRdd;

	}

}
