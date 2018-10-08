package cn.mastercom.bigdata.util.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class HiveHelper
{
	/**
	 * 将hive表中的结果转换成StringRDD
	 * 
	 * @param sql
	 * @param hiveContext
	 * @return
	 */
	public static JavaRDD<String> SearchRddByHive(String sql, HiveContext hiveContext)
	{
		System.out.println("Excute SQL: " + sql);
		JavaRDD<String> stringRdd = null;
		DataFrame hiveData = hiveContext.sql(sql);
		JavaRDD<Row> Row = hiveData.javaRDD();
		stringRdd = Row.map(new Function<Row, String>()
		{
			int rowCount = 0;
			StringBuilder tmSb = new StringBuilder();

			@Override
			public String call(Row row) throws Exception
			{
				// TODO Auto-generated method stub
				rowCount = row.length();
				tmSb.delete(0, tmSb.length());

				Object temp;
				for (int i = 0; i < rowCount - 1; i++)
				{
					temp = row.get(i);
					if (temp == null)
					{
						temp = "";
					}
					tmSb.append(temp.toString());
					tmSb.append("\t");
				}
				temp = row.get(rowCount - 1);
				if (temp == null)
				{
					temp = "";
				}
				tmSb.append(temp.toString());
				return tmSb.toString();
			}
		});
		return stringRdd;
	}
	
	
	public static JavaRDD<Row> SearchRddRowByHive(String sql, HiveContext hiveContext) throws Exception
	{
		System.out.println("Excute SQL: " + sql);
		
		DataFrame hiveData = hiveContext.sql(sql);
		JavaRDD<Row> Rows = hiveData.javaRDD();
		return Rows;
	}
	
	public static String RowToString(StringBuffer tmSb, Row row)
	{	
		tmSb.delete(0, tmSb.length());
		
		int rowCount = row.length();
		Object temp;
		for (int i = 0; i < rowCount - 1; i++)
		{
			temp = row.get(i);
			if (temp == null)
			{
				temp = "";
			}
			tmSb.append(temp.toString());
			tmSb.append("\t");
		}
		temp = row.get(rowCount - 1);
		if (temp == null)
		{
			temp = "";
		}
		tmSb.append(temp.toString());
		return tmSb.toString();		
	}


	
	

	
	
	
	
}
