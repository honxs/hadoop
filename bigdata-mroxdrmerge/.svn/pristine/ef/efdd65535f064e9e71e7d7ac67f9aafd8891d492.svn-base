package cn.mastercom.bigdata.stat.freqloc;

import java.util.ArrayList;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.GisFunction;

import org.apache.hadoop.io.Text;

public class FreqLocCluster
{
	private static int FreqLocDistance = MainModel.GetInstance().getAppConfig().getFreqLocDistance();

	public static ArrayList<FreqCellStruct> cluster(Iterable<Text> values)
	{
		FreqCellStruct firstStation; // 第一个基站
		FreqCellItem freqCell;
		FreqCellItem tempFreqCell;
		FreqCellStruct tempStation;
		long tempDistance = 0; // 缓存距离
		long minDistance = -1; // 最小距离
		int minIndex = 0; // 最小距离对应索引

		ArrayList<FreqCellStruct> list = new ArrayList<>(); // 没包数据一个基站装进list
		String vals[] = null;

		for (Text temp : values)
		{
			vals = temp.toString().split("\t", -1);
			int rsrp = Integer.parseInt(vals[4]);
			int ibuildingID = Integer.parseInt(vals[2]);
			int longitude = Integer.parseInt(vals[0]);
			int latitude = Integer.parseInt(vals[1]);
			int pci = Integer.parseInt(vals[5]);

			// 找最小距离对应的基站在list中的索引
			for (int i = 0; i < list.size(); i++)
			{
				tempDistance = (long) GisFunction.GetDistance(list.get(i).aveLongitude, list.get(i).aveLatitude,
						longitude, latitude);
				if (minDistance == -1)
				{
					minIndex = 0;
					minDistance = tempDistance;
				}
				else if (minDistance > tempDistance)
				{
					minIndex = i;
					minDistance = tempDistance;
				}
			}

			freqCell = new FreqCellItem();
			freqCell.fillData(vals);
			// 第一个采样点做第一个基站，超过阈值的也新建一个基站，更新中心经纬度并做统计。
			if (minDistance == -1 || minDistance > FreqLocDistance) // 超出距离
			{
				firstStation = new FreqCellStruct();
				firstStation.map.put(pci, freqCell);
				freqCell.doSample(rsrp, ibuildingID);
				firstStation.updateData(longitude, latitude);
				list.add(firstStation);
			}
			// 并入list已存储的基站的map中，更新中心经纬度并做统计。
			else
			{
				tempStation = list.get(minIndex);
				tempFreqCell = tempStation.map.get(pci);
				if (tempFreqCell == null) // 没有该pci
				{
					tempFreqCell = freqCell;
					tempStation.map.put(pci, tempFreqCell);
				}
				tempFreqCell.doSample(rsrp, ibuildingID);
				tempStation.updateData(longitude, latitude);
			}
		}
		return list;
	}
}
