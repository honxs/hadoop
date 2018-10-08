package cn.mastercom.bigdata.util;

public class MrLocation
{
	public static double calcDist(int LteScTadv, int LteScRTTD)
    {
        if (LteScTadv >= 0 && LteScTadv < 1282)
        {
            return (LteScTadv + 0.5) * 16 * 4.89;
        }
        else if (LteScRTTD <= 2046)
        {
            return (LteScRTTD + 0.5) * 2 * 4.89;
        }
        else if (LteScRTTD == 2047)
        {
            return (LteScRTTD + 2048) * 4.89;
        }
        else
        {
            return (2048 * 2 + (LteScRTTD - 2048 + LteScRTTD - 2048 + 1) * 4) * 4.89;
        }
    }

	/*	  
	 * longitude 基站经度
	 * latitude  基站纬度
	 * InDoor 是否室内站
	 * Angle  小区方向角
	 */
    public static MrLocationItem calcLongLac(int LteScTadv, int LteScRTTD, int LteScAOA,
    		double longitude, double latitude,boolean InDoor, int Angle, boolean AddRandom)
    {
    	if(LteScAOA<0 || LteScAOA>720)
    		return null;
    	
    	if(LteScTadv<0 && LteScRTTD<0)
    		return null;
   	
        double x = 0, y = 0;
        if (!InDoor)
        {
            double dist = calcDist(LteScTadv, LteScRTTD);
            if(AddRandom)
            {
            	dist += (Math.random() * 80-40);
            }
            if (dist > 2400)
            {
                return null;
            }
            
            double randomVal = 0;
            if(AddRandom)
            {
            	randomVal = Math.random() - 0.5;
            	if(LteScAOA +randomVal <0)
            	{
            		randomVal = 0;
            	}
            }
            double r = ((LteScAOA +randomVal)  / 2 + 0.25 + Angle) * Math.PI / 180;

            x = dist * Math.sin(r);
            y = dist * Math.cos(r);
        }
        MrLocationItem loc = new MrLocationItem();
        loc.longitude = (int)(longitude + x * 100);
        loc.latitude = (int) (latitude  + y * 90);
        return loc;
    }
    
    public static void test() {
        int a[] = new int[10];
        for (int i = 0; i <= 5; i++) {
            a[i] = (int) (Math.random() * 10)-5;
        }
        for (int i = 0; i < 5;)
            System.out.println(i + " : " + a[i++]);//代码中最好不要出现中文
    }
    
	public static void main(String[] args)
	{//1317838450	474014110	160
		MrLocation.calcLongLac(5, -1, 578, 131.9191449, 47.305469,false, 180,true);

	}

}

class Location
{
	public int longitude;
	public int latitude;
}
