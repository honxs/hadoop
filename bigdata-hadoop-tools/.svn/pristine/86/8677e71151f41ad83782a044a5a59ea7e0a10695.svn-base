package cn.mastercom.bigdata.util.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class RDDTypeResult implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private JavaPairRDD<TypeInfo, Iterable<String>> rddResult;
	private MapFilterFunc mapFilterFunc;
	private PairMapFilterFunc pairMapFilterFunc;
	private TypeInfoMng typeInfoMng;
	
	public RDDTypeResult(TypeInfoMng typeInfoMng, JavaPairRDD<TypeInfo, Iterable<String>> rddResult)
	{
		this.rddResult = rddResult;
		this.typeInfoMng = typeInfoMng;
	}
	
	public JavaRDD<String> getResult(List<Integer> typeList)
	{
		mapFilterFunc = new MapFilterFunc(typeList);
		JavaRDD<String> newRdd = rddResult.flatMap(mapFilterFunc);
		return newRdd;	
	}
	
	public JavaRDD<String> getResult(int typeID)
	{
		List<Integer> typeList = new ArrayList<Integer>();
		typeList.add(typeID);
		mapFilterFunc = new MapFilterFunc(typeList);
		JavaRDD<String> newRdd = rddResult.flatMap(mapFilterFunc);
		return newRdd;	
	}
	
	public JavaPairRDD<TypeInfo, Iterable<String>> getPairResult(List<Integer> typeList)
	{
		pairMapFilterFunc = new PairMapFilterFunc(typeList);
		JavaPairRDD<TypeInfo, Iterable<String>> newRdd = rddResult.mapToPair(pairMapFilterFunc);
		return newRdd;	
	}
	
	public class MapFilterFunc implements org.apache.spark.api.java.function.FlatMapFunction<Tuple2<TypeInfo, Iterable<String>>, String>
	{
		private static final long serialVersionUID = 1L;
		
		private boolean[] typeFlags;
		
		public MapFilterFunc(List<Integer> typeList)
		{
			typeFlags = new boolean[1000];
			for(int i =0; i<typeFlags.length; ++i)
			{		
				typeFlags[i] = false;
			}
			
			for(Integer dataType : typeList)
			{
				if(typeInfoMng.getTypeInfo(dataType) == null)
				{
					continue;
				}
				
				if(dataType > typeFlags.length-1 || dataType < 0)
				{
					continue;
				}
				typeFlags[dataType] = true;
			}
		}

		@Override
		public Iterable<String> call(Tuple2<TypeInfo, Iterable<String>> t)
				throws Exception
		{
			if(t._1.getType() > typeFlags.length || t._1.getType() < 0)
			{
				return new ArrayList<String>();
			}
			
			if(typeFlags[t._1.getType()])
			{
				return t._2;
			}
			return new ArrayList<String>();
		}
			
	}
	
	
	public class PairMapFilterFunc implements org.apache.spark.api.java.function.PairFunction<Tuple2<TypeInfo,Iterable<String>>,TypeInfo,Iterable<String>>
	{
		private static final long serialVersionUID = 1L;
		
		private boolean[] typeFlags;
		
		public PairMapFilterFunc(List<Integer> typeList)
		{
			typeFlags = new boolean[1000];
			for(int i =0; i<typeFlags.length; ++i)
			{		
				typeFlags[i] = false;
			}
			
			for(Integer dataType : typeList)
			{
				if(typeInfoMng.getTypeInfo(dataType) == null)
				{
					continue;
				}
				
				if(dataType > typeFlags.length-1 || dataType < 0)
				{
					continue;
				}
				typeFlags[dataType] = true;
			}
		}

		@Override
		public Tuple2<TypeInfo, Iterable<String>> call(Tuple2<TypeInfo, Iterable<String>> t)
				throws Exception
		{
			if(t._1.getType() > typeFlags.length || t._1.getType() < 0)
			{
				//return new Tuple2<TypeInfo, Iterable<String>>(null,new ArrayList<String>());
				return new Tuple2<TypeInfo, Iterable<String>>(null,null);
			}
			
			if(typeFlags[t._1.getType()])
			{
				return t;
			}
		    return new Tuple2<TypeInfo, Iterable<String>>(null,null);
		}
			
	}

}
