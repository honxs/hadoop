package cn.mastercom.bigdata.locsimu;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FillBack
{
	public static HashMap<String, ArrayList<String>> dataBase;
	public static ArrayList<String> referenceList;

	public static class EAMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>
	{
		private Path[] referFile;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			dataBase = new HashMap<String, ArrayList<String>>();
			referenceList = new ArrayList<String>();
			referFile = DistributedCache.getLocalCacheFiles(conf);
			Sample.readReferenceToCacheFile(referFile[0]);
			Sample.readBaseToCacheFile(referFile[1]);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] strs = value.toString().split("\t");
			if (strs[Sample.ilongitude].equals("0") || strs[Sample.ilatitude].equals("0"))
			{
				return;
			}
			HashMap<String, String> cell = new HashMap<String, String>();
			String longitude = null;
			String latitude = null;
			String mainCell = null;
			int div = Integer.parseInt(strs[Sample.Eci]) / 256;
			int mod = Integer.parseInt(strs[Sample.Eci]) % 256;

			for (int i = 0; i < referenceList.size(); i++)
			{
				String[] reference = referenceList.get(i).toString().split("\t");
				if (div == Integer.parseInt(reference[1]) && mod == Integer.parseInt(reference[6]))
				{
					longitude = reference[2];
					latitude = reference[3];
					mainCell = reference[5];
					cell.put(reference[5], latitude + "," + longitude + "_" + strs[Sample.LteScRSRP]);
					break;
				}
			}
			if (longitude == null || latitude == null || mainCell == null)
			{
				return;
			}

			for (int j = Sample.LteNcEarfcn1; j <= Sample.LteNcEarfcn6; j = j + 4)
			{
				if (strs[j].equals("-1000000") || strs[j + 1].equals("-1000000") || strs[j - 2].equals("-1000000"))
				{
					break;
				}
				double minDistance = 100000000.0;
				int id = -1;
				for (int i = 0; i < referenceList.size(); i++)
				{
					String[] reference = referenceList.get(i).toString().split("\t");
					if (strs[j + 1].equals(reference[7]) && strs[j].equals(reference[8]))
					{
						double tempDistance = Sample.getDist(Double.parseDouble(reference[2]),
								Double.parseDouble(reference[3]), Double.parseDouble(longitude),
								Double.parseDouble(latitude));
						if (tempDistance < minDistance)
						{
							minDistance = tempDistance;
							id = i;
						}
					}
				}
				if (id != -1)
				{
					String[] finalRefer = referenceList.get(id).toString().split("\t");
					cell.put(finalRefer[5], finalRefer[3] + "," + finalRefer[2] + "_" + strs[j - 2]);
				}
			}

			// location estimation
			Double[] es = Sample.LocationEstimation(cell);

			double error = Sample.getDist(Double.parseDouble(strs[Sample.ilongitude]) / 10000000,
					Double.parseDouble(strs[Sample.ilatitude]) / 10000000, es[0], es[1]);
			context.write(new IntWritable(cell.size()), new DoubleWritable(error));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			referenceList.clear();
			dataBase.clear();
		}
	}

	public static class EAReducer extends Reducer<IntWritable, DoubleWritable, DoubleWritable, IntWritable>
	{
		@Override
		protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException
		{
			for (DoubleWritable dw : values)
			{
				context.write(dw, key);
			}
		}
	}

	public int run(String[] args, Configuration conf) throws Exception
	{
		conf.set("mapreduce.job.queuename", "network");
		conf.setBoolean("mapred.compress.map.output", true);
		conf.setBoolean("mapred.output.compress", true);
		conf.setIfUnset("mapred.output.compression.type", "BLOCK");
		conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
		String[] otherArgsStrings = new GenericOptionsParser(conf, args).getRemainingArgs();

		DistributedCache.addCacheFile(new URI(otherArgsStrings[1]), conf);
		DistributedCache.addCacheFile(new URI(otherArgsStrings[2]), conf);
		Job job = new Job(conf, FillBack.class.getSimpleName());
		job.setJarByClass(FillBack.class);

		job.setMapperClass(EAMapper.class);
		// job.setCombinerClass(EAReducer.class);
		job.setReducerClass(EAReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgsStrings[3]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgsStrings[0]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private static void fileMerge(Path path, FileSystem hdfs, FSDataOutputStream out)
	{
		try
		{
			FileStatus[] listStatus = hdfs.listStatus(path);
			for (int i = 0; i < listStatus.length; i++)
			{
				FSDataInputStream in = hdfs.open(listStatus[i].getPath());
				byte buffer[] = new byte[1024];
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) > 0)
				{
					out.write(buffer, 0, bytesRead);
				}
				in.close();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception
	{
		// Configuration conf = new Configuration();
		// Path inputDir = new Path("hdfs://node001:9000/");
		// FileSystem hdfs = FileSystem.get(conf);
		// FSDataOutputStream out = hdfs.create(inputDir);
		// for (int i = 0; i < data.length; i++) {
		// FileStatus[] listStatus = hdfs.listStatus(new Path(data[i]));
		// for (int j = 0; j < listStatus.length; j++) {
		// String dirS = listStatus[j].getPath().toString();
		// if (dirS.contains("TB_DTSIGNAL_SAMPLE")) {
		// fileMerge(listStatus[j].getPath(), hdfs, out);
		// }
		// }
		// }
		//
		// int status = new ErrorAnalysis().run(args, conf);
		// System.exit(status);
	}
}
