package cn.mastercom.bigdata.locsimu;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;

public class Sample {
	public static final int ilongitude = 6;
	public static final int ilatitude = 7;
	public static final int Eci = 12;
	public static final int LteScRSRP = 48;
	public static final int LteNcRSRP1 = 63;
	public static final int LteNcEarfcn1 = 65;
	public static final int LteNcPci1 = 66;
	public static final int LteNcRSRP2 = 67;
	public static final int LteNcEarfcn2 = 69;
	public static final int LteNcPci2 = 70;
	public static final int LteNcRSRP3 = 71;
	public static final int LteNcEarfcn3 = 73;
	public static final int LteNcPci3 = 74;
	public static final int LteNcRSRP4 = 75;
	public static final int LteNcEarfcn4 = 77;
	public static final int LteNcPci4 = 78;
	public static final int LteNcRSRP5 = 79;
	public static final int LteNcEarfcn5 = 81;
	public static final int LteNcPci5 = 82;
	public static final int LteNcRSRP6 = 83;
	public static final int LteNcEarfcn6 = 85;
	public static final int LteNcPci6 = 86;

	public static double getDist(double longitude, double latitude,
			double weightLongitude, double weightLatitude) {
		final int EarthRadius = 6378137;
		latitude = latitude * Math.PI / 180.0;
		weightLatitude = weightLatitude * Math.PI / 180.0;
		double a = latitude - weightLatitude;
		double b = (longitude - weightLongitude) * Math.PI / 180.0;
		double sa2, sb2;
		sa2 = Math.sin(a / 2.0);
		sb2 = Math.sin(b / 2.0);
		double d = 2
				* EarthRadius
				* Math.asin(Math.sqrt(sa2 * sa2 + Math.cos(latitude)
						* Math.cos(weightLatitude) * sb2 * sb2));
		return d;
	}

	public static void readReferenceToCacheFile(Path path) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					new File(path.toString())), "UTF-8"));
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
				FillBack.referenceList.add(strs[0] + "\t" + strs[3] + "\t"
						+ strs[4] + "\t" + strs[5] + "\t" + strs[6] + "\t"
						+ strs[9] + "\t" + strs[10] + "\t" + strs[13] + "\t"
						+ strs[14]);
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
	}

	public static void readBaseToCacheFile(Path path) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					new File(path.toString())), "UTF-8"));
			String line;
			String mainCell = null;
			while ((line = br.readLine()) != null) {
				int splitPosition = line.indexOf("\t");
				String startString = line.substring(0, splitPosition);
				String endString = line.substring(splitPosition + 1);
				if (startString.equals(mainCell)) {
					ArrayList<String> arrayList = FillBack.dataBase
							.get(mainCell);
					arrayList.add(endString);
				} else {
					mainCell = startString;
					ArrayList<String> arrayList = new ArrayList<String>();
					arrayList.add(endString);
					FillBack.dataBase.put(startString, arrayList);
				}
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
	}

	public static Double[] LocationEstimation(HashMap<String, String> cell) {
		HashSet<String> hs = new HashSet<String>();
		ArrayList<String> tempData = new ArrayList<String>();
		Iterator<Map.Entry<String, String>> it = cell.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> next = it.next();
			hs.add((next.getValue().split("_"))[0]);

			ArrayList<String> al = FillBack.dataBase.get(next.getKey());
			if (al != null) {
				tempData.addAll(al);
			}
		}
		if (hs.size() < 3 || tempData.isEmpty()) {
			return geometricCenter(hs);
		}
		
		int matchCellCount = 0;
		ArrayList<String> tempArrayList = new ArrayList<String>();
		for (int i = 0; i < tempData.size(); i++) {
			String[] split = tempData.get(i).split("\t");
			HashMap<String, String> cellMap = new HashMap<String, String>();
			for (int k = 1; k < split.length - 1; k++) {
				cellMap.put(split[k], split[k + 1]);
			}
			Iterator<Map.Entry<String, String>> iterator1 = cell.entrySet()
					.iterator();
			int tempCount = 0;
			while (iterator1.hasNext()) {
				Map.Entry<String, String> next = iterator1.next();
				String key2 = next.getKey();
				double value2 = Double.valueOf((next.getValue().split("_"))[1]);
				if (cellMap.containsKey(key2)) {
					String[] RSRPRange = ((cellMap.get(key2).split("_"))[1])
							.split(":");
					if (value2 >= Double.valueOf(RSRPRange[0])
							&& value2 <= Double.valueOf(RSRPRange[1])) {
						tempCount++;
					}
				}
			}
			if (tempCount > matchCellCount) {
				matchCellCount = tempCount;
				tempArrayList.clear();
				tempArrayList.add(split[0]);
			} else if (tempCount == matchCellCount) {
				tempArrayList.add(split[0]);
			}
		}
		if (matchCellCount < 3) {
			return geometricCenter(hs);
		} else {
			double estiLong = 0.0;
			double estiLat = 0.0;
			for (int i = 0; i < tempArrayList.size(); i++) {
				String[] longLat = tempArrayList.get(i).split(",");
				estiLong += Double.valueOf(longLat[1]);
				estiLat += Double.valueOf(longLat[0]);
			}
			return new Double[] { estiLong / tempArrayList.size(),
					estiLat / tempArrayList.size() };
		}
	}

	private static Double[] geometricCenter(HashSet<String> hs) {
		Iterator<String> iterator = hs.iterator();
		double lon = 0.0;
		double lat = 0.0;
		while (iterator.hasNext()) {
			String[] lonLat = iterator.next().split(",");
			lon += Double.parseDouble(lonLat[1]);
			lat += Double.parseDouble(lonLat[0]);
		}
		return new Double[] { lon / hs.size(), lat / hs.size() };
	}
}
