package cn.mastercom.bigdata.mroxdrmerge;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.MroOrigDataMT;
import cn.mastercom.bigdata.StructData.NC_GSM;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.StructData.NC_TDS;
import cn.mastercom.bigdata.StructData.SC_FRAME;
import cn.mastercom.bigdata.StructData.SIGNAL_MR_All;
import cn.mastercom.bigdata.StructData.SIGNAL_MR_SC;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.StructData.XdrLocation;
import cn.mastercom.bigdata.mro.loc.MroXdrDeal;
import cn.mastercom.bigdata.mro.loc.hsr.stat.Stat_TrainSegCell;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.spark.TypeInfo;
import cn.mastercom.bigdata.util.spark.TypeInfoMng;
import cn.mastercom.bigdata.util.spark.TypeResult;
import cn.mastercom.bigdata.xdr.loc.XdrLocDeal;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
import scala.Tuple2;

public class MyKryoRegistrator implements KryoRegistrator
{

	@SuppressWarnings("rawtypes")
	@Override
	public void registerClasses(Kryo kryo)
	{
		//在Kryo序列化库中注册自定义的类
		kryo.register(String.class);  
		kryo.register(Integer.class); 
		kryo.register(Long.class);  
		kryo.register(Double.class);  
		kryo.register(Boolean.class);
		kryo.register(List.class);
		kryo.register(ArrayList.class);  
		kryo.register(HashMap.class);
		kryo.register(Serializable.class);
		kryo.register(Tuple2.class);
		
		kryo.register(XdrLocDeal.class);
		kryo.register(MroXdrDeal.class);
		kryo.register(ResultOutputer.class);
		kryo.register(TypeInfo.class);   
		kryo.register(TypeInfoMng.class);    
		kryo.register(TypeResult.class);   		
		kryo.register(XdrLocation.class);   
		kryo.register(GridItem.class); 
		kryo.register(SIGNAL_MR_All.class);
		kryo.register(ParseItem.class);
		kryo.register(NC_TDS.class);
		kryo.register(NC_LTE.class);
		kryo.register(NC_GSM.class);
		kryo.register(SIGNAL_MR_SC.class);
		kryo.register(SC_FRAME.class); 
		kryo.register(StaticConfig.class);
		kryo.register(MroOrigDataMT.class);
		kryo.register(Stat_TrainSegCell.class);
	}

}
