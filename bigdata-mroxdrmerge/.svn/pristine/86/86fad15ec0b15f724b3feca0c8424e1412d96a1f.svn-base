package cn.mastercom.bigdata.stat.freqloc;

import java.util.HashMap;
import java.util.Map;

/**
 * 基站
 * 
 * @author yzx
 */
public class FreqCellStruct
{
	public Map<Integer, FreqCellItem> map;

	public long totalLongitude; // 总经度
	public long totalLatitude; // 总纬度
	public int totalNum; // 个数
	public int aveLongitude; // 基站经度
	public int aveLatitude; // 基站纬度

	public FreqCellStruct()
	{
		clean();
	}

	public void clean()
	{
		map = new HashMap<Integer, FreqCellItem>();
		totalLongitude = 0;
		totalLatitude = 0;
		totalNum = 0;
		aveLongitude = 0;
		aveLatitude = 0;
	}

	public void updateData(int longitude, int latitude)
	{
		// 总经纬度累加，总个数累加
		totalLongitude += longitude;
		totalLatitude += latitude;
		totalNum++;
		// 更新基站的经纬度（中心经纬度）
		this.aveLongitude = (int) (totalLongitude / totalNum);
		this.aveLatitude = (int) (totalLatitude / totalNum);
	}

}
