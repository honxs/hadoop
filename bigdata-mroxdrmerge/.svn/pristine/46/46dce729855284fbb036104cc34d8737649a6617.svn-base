package cn.mastercom.bigdata.spark.mergestat;

import java.util.ArrayList;
import java.util.Arrays;

import cn.mastercom.bigdata.util.spark.TypeInfo;
import cn.mastercom.bigdata.util.spark.TypeInfoMng;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;


public class MapByTypeFunc_MergeData implements PairFlatMapFunction<Tuple2<MergeTypeItem, Iterable<String>>, TypeInfo, String>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private TypeInfoMng typeInfoMng;

	public MapByTypeFunc_MergeData(TypeInfoMng typeInfoMng)
	{
         this.typeInfoMng = typeInfoMng;
	}
	
	public TypeInfoMng getTypeInfoMng()
	{
		return typeInfoMng;
	}

	@Override
	public Iterable<Tuple2<TypeInfo, String>> call(Tuple2<MergeTypeItem, Iterable<String>> t)
			throws Exception
	{
		MergeTypeInfo mergeTypeInfo = (MergeTypeInfo)typeInfoMng.getTypeInfo(t._1.getType());
		if(mergeTypeInfo == null)
		{
			return new ArrayList<Tuple2<TypeInfo, String>>();
		}
		
		MergeStat mergeStat = new MergeStat(mergeTypeInfo);
		mergeStat.deal(t._2);

		TypeInfo typeInfo = new TypeInfo(mergeTypeInfo.getType(), mergeTypeInfo.getTypeName(), mergeTypeInfo.getTypePath());
		Tuple2<TypeInfo, String> tp = new Tuple2<TypeInfo, String>(typeInfo, mergeStat.outResult());
		return Arrays.asList(tp);
	}

}
