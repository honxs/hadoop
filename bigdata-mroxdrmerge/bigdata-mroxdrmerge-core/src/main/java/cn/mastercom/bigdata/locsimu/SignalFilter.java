package cn.mastercom.bigdata.locsimu;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SignalFilter {
	public static class LocationMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text infoKeyText = new Text();
		private Text infoValueText = new Text();
		private HashMap<String, HashSet<String>> mainCellLocation = new HashMap<String, HashSet<String>>();
		private HashMap<String, HashMap<String, String>> locationCellRSRP = new HashMap<String, HashMap<String, String>>();
		private ArrayList<String> referenceList;
		private Path[] referFile;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			referenceList = new ArrayList<String>();
			Configuration conf = context.getConfiguration();
			referFile = DistributedCache.getLocalCacheFiles(conf);
			if (referFile != null && referFile.length > 0) {
				referenceList = readCacheFile(referFile[0]);
			}
		}

		private ArrayList<String> readCacheFile(Path referFile) {
			ArrayList<String> alist = new ArrayList<String>();
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(
						new FileInputStream(new File(referFile.toString())),
						"UTF-8"));
				String line;
				while ((line = br.readLine()) != null) {
					if (line.contains("CellID"))
						continue;
					String[] strs = line.split("\t");
					if ("".equals(strs[0]) || "".equals(strs[3])
							|| "".equals(strs[4]) || "".equals(strs[5])
							|| "".equals(strs[6]) || "".equals(strs[9])
							|| "".equals(strs[10]) || "".equals(strs[13])
							|| "".equals(strs[14])) {
						continue;
					}
					alist.add(strs[0] + "\t" + strs[3] + "\t" + strs[4] + "\t"
							+ strs[5] + "\t" + strs[6] + "\t" + strs[9] + "\t"
							+ strs[10] + "\t" + strs[13] + "\t" + strs[14]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e1) {
					}
				}
			}
			return alist;
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");

			if (strs[Sample.ilongitude].equals("0")
					|| strs[Sample.ilatitude].equals("0")) {
				return;
			}
			if (strs[Sample.LteNcEarfcn2].equals("-1000000")
					|| strs[Sample.LteNcRSRP2].equals("-1000000")
					|| strs[Sample.LteNcPci2].equals("-1000000")) {
				return;
			}
			HashMap<String, String> cell = new HashMap<String, String>();

			String location = strs[Sample.ilatitude] + ","
					+ strs[Sample.ilongitude]; // change
			String mainCell = null;
			// String mainRSRP = null;
			// search for main cell
			String longitude = null;
			String latitude = null;
			int div = Integer.parseInt(strs[Sample.Eci]) / 256;
			int mod = Integer.parseInt(strs[Sample.Eci]) % 256;

			for (int i = 0; i < referenceList.size(); i++) {
				String[] reference = referenceList.get(i).toString()
						.split("\t");
				if (div == Integer.parseInt(reference[1])
						&& mod == Integer.parseInt(reference[6])) {
					longitude = reference[2];
					latitude = reference[3];
					mainCell = reference[5];
					// mainRSRP = strs[Sample.LteScRSRP];
					cell.put(mainCell, latitude + "," + longitude + "_"
							+ strs[Sample.LteScRSRP] + ":" // change
							+ strs[Sample.LteScRSRP]);
					break;
				}
			}
			if (longitude == null || latitude == null || mainCell == null) {
				return;
			}
			if (mainCellLocation.containsKey(mainCell)) {
				mainCellLocation.get(mainCell).add(location);
			} else {
				HashSet<String> al = new HashSet<String>();
				al.add(location);
				mainCellLocation.put(mainCell, al);
			}
			// search for neighbour cell
			for (int j = Sample.LteNcEarfcn1; j <= Sample.LteNcEarfcn6; j = j + 4) {
				// String LteNcEarfcn = strs[j];
				// String LteNcPci = strs[j + 1];
				// String LteNcRSRP = strs[j - 2];
				if (strs[j].equals("-1000000")
						|| strs[j + 1].equals("-1000000")
						|| strs[j - 2].equals("-1000000")) {
					break;
				}
				double minDistance = 100000000.0;
				int id = -1;
				for (int i = 0; i < referenceList.size(); i++) {
					String[] reference = referenceList.get(i).toString()
							.split("\t");
					if (strs[j + 1].equals(reference[7])
							&& strs[j].equals(reference[8])) {
						double tempDistance = Sample.getDist(
								Double.parseDouble(reference[2]),
								Double.parseDouble(reference[3]),
								Double.parseDouble(longitude),
								Double.parseDouble(latitude));
						if (tempDistance < minDistance) {
							minDistance = tempDistance;
							id = i;
						}
					}
				}
				if (id != -1) {
					String[] finalRefer = referenceList.get(id).toString()
							.split("\t");
					// String neighbourCell = finalRefer[5];
					cell.put(finalRefer[5], finalRefer[3] + "," + finalRefer[2]
							+ "_" + strs[j - 2] + ":" + strs[j - 2]); // change
				}
			}
			if (locationCellRSRP.containsKey(location)) {
				HashMap<String, String> locationCellMap = locationCellRSRP
						.get(location);
				Iterator<Map.Entry<String, String>> iterator = cell.entrySet()
						.iterator();
				while (iterator.hasNext()) {
					Map.Entry<String, String> me = iterator.next();
					String cellName = me.getKey();
					String[] latLonCellRSRP = me.getValue().split("_");
					if (locationCellMap.containsKey(cellName)) {
						String[] oldSplit = locationCellMap.get(cellName)
								.split("_")[1].split(":"); // change
						String[] newSplit = latLonCellRSRP[1].split(":"); // change
						String min = Double.toString(Math.min(
								Double.valueOf(oldSplit[0]),
								Double.valueOf(newSplit[0])));
						String max = Double.toString(Math.max(
								Double.valueOf(oldSplit[1]),
								Double.valueOf(newSplit[1])));
						locationCellMap.put(cellName, latLonCellRSRP[0] + "_"
								+ min + ":" + max); // change
					} else {
						locationCellMap.put(cellName, me.getValue()); // change
					}
				}
			} else {
				locationCellRSRP.put(location, cell);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// join two map together(mainCellLocation,locationCellRSRP)
			Iterator<Map.Entry<String, HashSet<String>>> iterator = mainCellLocation
					.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<String, HashSet<String>> me = iterator.next();
				String cellName = me.getKey();
				infoKeyText.set(cellName);
				HashSet<String> locationList = me.getValue();
				Iterator<String> iterator2 = locationList.iterator();
				while (iterator2.hasNext()) {
					String location = iterator2.next();
					StringBuilder sb = new StringBuilder();
					sb.append(location);
					HashMap<String, String> hashMap = locationCellRSRP
							.get(location);
					Iterator<Map.Entry<String, String>> it = hashMap.entrySet()
							.iterator();
					while (it.hasNext()) {
						Map.Entry<String, String> next = it.next();
						sb.append("\t" + next.getKey() + "\t" + next.getValue());
					}
					infoValueText.set(sb.toString());
					context.write(infoKeyText, infoValueText);
					sb.delete(0, sb.length());
				}
			}
			mainCellLocation.clear();
			locationCellRSRP.clear();
		}

	}

	public static class LocationReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}

	public int run(String[] args, Configuration conf) throws Exception {
		conf.set("mapreduce.job.queuename", "network");
		String[] otherArgsStrings = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		DistributedCache.addCacheFile(new URI(otherArgsStrings[1]), conf);
		Job job = new Job(conf, SignalFilter.class.getSimpleName());
		job.setJarByClass(SignalFilter.class);
		job.setMapperClass(LocationMapper.class);
		job.setReducerClass(LocationReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPaths(job, otherArgsStrings[2]);
		FileOutputFormat.setOutputPath(job, new Path(otherArgsStrings[0]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private static void fileMerge(Path path, FileSystem hdfs,
			FSDataOutputStream out) {
		try {
			FileStatus[] listStatus = hdfs.listStatus(path);
			for (int i = 0; i < listStatus.length; i++) {
				FSDataInputStream in = hdfs.open(listStatus[i].getPath());
				byte buffer[] = new byte[1024];
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) > 0) {
					out.write(buffer, 0, bytesRead);
				}
				in.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
//		Path inputDir = new Path("hdfs://node001:9000/xu/test/in/data");
//		Configuration conf = new Configuration();
//		FileSystem hdfs = FileSystem.get(conf);
//		FSDataOutputStream out = hdfs.create(inputDir);
//		for (int i = 0; i < data.length; i++) {
//			FileStatus[] listStatus = hdfs.listStatus(new Path(data[i]));
//			for (int j = 0; j < listStatus.length; j++) {
//				String dirS = listStatus[j].getPath().toString();
//				if (dirS.contains("TB_CQTSIGNAL_SAMPLE")
//						|| dirS.contains("TB_DTSIGNAL_SAMPLE")) {
//					fileMerge(listStatus[j].getPath(), hdfs, out);
//				}
//			}
//		}
//		out.close();
//		int status = new SignalFilter().run(args, conf);
//		System.exit(status);
	}
}
