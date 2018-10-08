package cn.mastercom.bigdata.util.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import scala.Tuple2;

public class RowRDDFunc implements FlatMapFunction<Tuple2<TypeInfo, Iterable<String>>, Row>
{
	private static final long serialVersionUID = 1L;
	
//	private boolean[] typeFlags = new boolean[1000];
	private ArrayList<Integer> list = new ArrayList<>();
	private List<Row> rowList = new ArrayList<Row>();
	
	public RowRDDFunc(List<Integer> saveDataList)
	{
		//init save list		
//		for(int i = 0; i<typeFlags.length; ++i)
//		{
//			typeFlags[i] = false; 
//		}
		
		for(int i : saveDataList)
		{
//			typeFlags[i] = true;
			list.add(i);
		}
	}

	@Override
	public Iterable<Row> call(Tuple2<TypeInfo, Iterable<String>> tp) throws Exception
	{
		rowList.clear();
		try
		{
//		    if(tp._1.type < 0 || tp._1.type >= typeFlags.length)
//		    {
//		    	return rowList;
//		    }
			if(!list.contains(tp._1.type))
			{
				return rowList;
			}
			else
			{
				for (String data : tp._2)
				{
					data = data.replace("\n", "==>");
					String typeName = tp._1.typePath.substring(tp._1.typePath.lastIndexOf("/")+1);
					Row row = RowFactory.create(typeName, data);
//					Row row = RowFactory.create(tp._1.typeName, data);
					rowList.add(row);
				}
			}
			return rowList;
		}
		catch (Exception e)
		{
			return rowList;
		}
	}
	
}