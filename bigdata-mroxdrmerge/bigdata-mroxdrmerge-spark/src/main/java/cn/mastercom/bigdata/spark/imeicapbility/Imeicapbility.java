package cn.mastercom.bigdata.spark.imeicapbility;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class Imeicapbility
{
	private static void DoSparkJob(String[] args)
	{
		String imeiPath = "/mt_wlyh/Data/config/ImeiCapbility";
		HDFSOper hdfsOper = null;
		try
		{
			hdfsOper = new HDFSOper("hdfs://192.168.1.31:9000");
		}
		catch (Exception e1)
		{
			e1.printStackTrace();
		}
		SparkConf sparkConf = new SparkConf().setAppName("Imeicapbility").setMaster("local");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> newLines = ctx.textFile(args[0]);
		JavaRDD<String> oldLines;
		if (!hdfsOper.checkFileExist(imeiPath))
		{
			hdfsOper.mkfile(imeiPath);
		}
		oldLines = ctx.textFile(imeiPath);

		JavaPairRDD<String, Integer> newImeiToType = newLines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>()
		{
			@Override
			public Iterable<Tuple2<String, Integer>> call(String s) throws Exception
			{
				String[] split = s.split("\t");
				DT_Sample_4G dt_sample_4G = new DT_Sample_4G();
				try
				{
//					dt_sample_4G.fileData(split);
//					List<NC_LTE> nclte_freq = dt_sample_4G.getNclte_Freq();
					int isLT = 0;
					int isDX = 0;
//					for (int i = 0; i < nclte_freq.size(); i++)
//					{
//						int lteNcEarfcn = nclte_freq.get(i).LteNcEarfcn;
//						if (lteNcEarfcn == 1600 || lteNcEarfcn == 1650 || lteNcEarfcn == 40340)
//						{
//							isLT = 2;
//						}
//						else if (lteNcEarfcn == 1775 || lteNcEarfcn == 1800 || lteNcEarfcn == 1825 || lteNcEarfcn == 1850 || lteNcEarfcn == 75 || lteNcEarfcn == 100)
//						{
//							isDX = 1;
//						}
//					}
					String imei = dt_sample_4G.imeiTac + "";
					if (imei.length() < 8)
					{
						return new ArrayList<Tuple2<String, Integer>>();
					}

					imei = imei.substring(0, 8);

					int i = isLT + isDX;
					ArrayList<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
					Tuple2<String, Integer> tp = new Tuple2<>(imei, i);
					tuple2s.add(tp);
					return tuple2s;
				}
				catch (Exception e)
				{
					return new ArrayList<Tuple2<String, Integer>>();
				}
			}
		});
		JavaPairRDD<String, Integer> oldImeiToType = oldLines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>()
		{
			@Override
			public Iterable<Tuple2<String, Integer>> call(String s) throws Exception
			{
				String[] split = s.split("\t");

				if (split.length != 2)
				{
					return new ArrayList<>();
				}

				String imei = split[0];
				int i = Integer.parseInt(split[1]);
				ArrayList<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
				Tuple2<String, Integer> tp = new Tuple2<>(imei, i);
				tuple2s.add(tp);
				return tuple2s;
			}
		});
		JavaPairRDD<String, Integer> unionRDD = newImeiToType.union(oldImeiToType);

		JavaPairRDD<String, Integer> reduceRDD = unionRDD.reduceByKey(new Function2<Integer, Integer, Integer>()
		{
			@Override
			public Integer call(Integer i1, Integer i2) throws Exception
			{
				return i1 | i2;
			}
		});

		// 由于数据量很小
		List<Tuple2<String, Integer>> collect = reduceRDD.collect();
		String text = "";
		for (Tuple2<String, Integer> tp : collect)
		{
			text += tp._1 + "\t" + tp._2 + "\n";
		}

		try
		{
			SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
			String format = df.format(new Date());

			hdfsOper.movefile(imeiPath, imeiPath + format);
			hdfsOper.writerString(text, imeiPath);

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void main(String[] args)
	{
		if (args.length < 1)
		{
			System.err.println("args.length<1");
		}
		else
		{
			DoSparkJob(args);
		}
	}

}
