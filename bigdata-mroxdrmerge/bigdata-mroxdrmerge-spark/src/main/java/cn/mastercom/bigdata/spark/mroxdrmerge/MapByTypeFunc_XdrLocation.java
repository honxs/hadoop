package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.util.ArrayList;
import java.util.List;

import cn.mastercom.bigdata.util.spark.MapByTypeFuncBase;
import cn.mastercom.bigdata.util.spark.TypeInfo;
import scala.Tuple2;


public class MapByTypeFunc_XdrLocation extends MapByTypeFuncBase  implements org.apache.spark.api.java.function.PairFlatMapFunction<String, TypeInfo, Iterable<String>>
{
	private static final long serialVersionUID = 1L;
	
	private int timeSpan = -1;
	private int curHour = 0;
	
	private int itime;
	private String strTime;
	private String[] valstrs;
	private int tmpInt;
	
	public static final int DataType_XDRLOCATION_SPAN = 1;
	
    public MapByTypeFunc_XdrLocation(Object[] args) throws Exception
    {
		super(args);
		
		this.curHour = (int)args[2];
    	this.timeSpan = (int)args[3];
    		
    	String tmHour = curHour < 10? "0"+curHour:""+curHour;
		String filename = "TB_XDRLOCATION_SPAN_" + dateStr + "_" + tmHour;
		TypeInfo typeInfo = new TypeInfo(DataType_XDRLOCATION_SPAN, filename, outPath + "/" + filename);
		typeInfoMng.registTypeInfo(typeInfo);	
    }
    
    @Override
    protected int init_once_sub() throws Exception
	{
    	return 0;
	}

	@Override
	public Iterable<Tuple2<TypeInfo, Iterable<String>>> call(String value) throws Exception
	{
		// 初始化配置
		init_once();
		
		valstrs = value.toString().split("\t", 5);
		if (valstrs.length < 5)
		{
			return new ArrayList<Tuple2<TypeInfo,Iterable<String>>>();
		}
		strTime = valstrs[3];
		if(strTime.length() != 10)
		{
			return new ArrayList<Tuple2<TypeInfo,Iterable<String>>>();
		}
		
		itime = Integer.parseInt(strTime);
		tmpInt = itime /3600*3600;
		if(itime >= tmpInt+3600-timeSpan)
		{
			typeResult.pushData(DataType_XDRLOCATION_SPAN, value);
		}
		
		List<Tuple2<TypeInfo, Iterable<String>>> resultRdd = typeResult.GetRdds();
		typeResult.clear();
		return resultRdd;
	}

    
    
    
    
    
    
}
