package cn.mastercom.bigdata.util;

public class GisFunction
{
    
    public static double GetDistance(double x1, double y1, double x2, double y2)
    {
        double longitudeDistance = (Math.sin((90 - y1) * 2 * Math.PI / 360) + Math.sin((90 - y2) * 2 * Math.PI / 360)) / 2 * (x1 - x2) / 360 * 40075360;
        double latitudeDistance = (y1 - y2) / 360 * 39940670;
        return (double)Math.sqrt(longitudeDistance * longitudeDistance + latitudeDistance * latitudeDistance);
    }
    
    public static double GetDistance(int x1, int y1, int x2, int y2)
    {
    	return GetDistance(x1/10000000.0, y1/10000000.0, x2/10000000.0, y2/10000000.0);
    }
    
    /// <summary>
    /// 获得由某经纬 到另一经纬度 的方向
    /// </summary>
    /// <param name="cellLong"></param>
    /// <param name="cellLat"></param>
    /// <param name="ptLong"></param>
    /// <param name="ptLat"></param>
    /// <returns></returns>
    public static int GetAngleFromPointToPoint(double orgLong, double orgLat, double tarLong, double tarLat)
    {
        double alpha = 0;
        double x1 = orgLong;
        double y1 = orgLat;
        double x2 = tarLong;
        double y2 = tarLat;
        double atan = Math.atan((y2 - y1) / (x2 - x1)) * 180 / Math.PI;

        if (x2 > x1 && y2 > y1)
        {
            alpha = 90 - atan;
        }
        else if (x2 > x1 && y2 < y1)
        {
            alpha = 90 - atan;
        }
        else if (x2 < x1 && y2 < y1)
        {
            alpha = 270 - atan;
        }
        else if (x2 < x1 && y2 > y1)
        {
            alpha = 270 - atan;
        }
        else if (x2 > x1 && y2 == y1)
        {
            alpha = 90;
        }
        else if (x2 < x1 && y2 == y1)
        {
            alpha = 270;
        }
        else if (x2 == x1 && y2 > y1)
        {
            alpha = 0;
        }
        else if (x2 == x1 && y2 < y1)
        {
            alpha = 180;
        }
        return (int)alpha;
    }

	public static int GetDirection(double orgLong, double orgLat, double tarLong, double tarLat)
	{
		int angle = GetAngleFromPointToPoint(orgLong, orgLat, tarLong, tarLat);
		if (angle > 45 && angle <= 135)
		{
			return 1;
		}
		else if(angle > 135 && angle <= 225)
		{
			return 2;
		}
		else if (angle >225 && angle <= 315)
		{
			return 3;
		}
		return 4;
	}

}
