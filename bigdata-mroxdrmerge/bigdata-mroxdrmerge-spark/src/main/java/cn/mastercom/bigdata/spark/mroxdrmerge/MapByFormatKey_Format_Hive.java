package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

public class MapByFormatKey_Format_Hive implements org.apache.spark.api.java.function.PairFlatMapFunction<String, String, String>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Iterable<Tuple2<String, String>> call(String value) throws Exception
	{
		// TODO Auto-generated method stub
		List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
		String[] vals = value.split(",|\t", -1);
		String key = vals[3] + "_" + vals[8] + "_" + vals[0] + "_" + vals[10];
		Tuple2 tuple = new Tuple2(key, value);
		list.add(tuple);
		return list;
	}

}
