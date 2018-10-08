package cn.mastercom.bigdata.StructData;

import java.util.Random;


public class RowKeyMaker
{
	
    public RowKeyMaker()
    {
    }

    public static String DtLteSampleKey(int randomMin, int randomMax, long imsi, int time, int timeMs, int longitude, int latitude)
    {
        String blockID = GetBlockID(longitude, latitude);
        int hashRegion = GetRegion(randomMin, randomMax, time, blockID);
              
        String key = String.format("%02d_%s_%010d_%015d_%010d_%03d_%04d", 
        		hashRegion, blockID, (time / 3600) * 3600, imsi, time, timeMs, GetRandom());
        return key;
    }

    public static String DtLteEventKey(int randomMin, int randomMax, long imsi, int time, int timeMs, int longitude, int latitude)
    {
        String blockID = GetBlockID(longitude, latitude);
        int hashRegion = GetRegion(randomMin, randomMax, time, blockID);
        
        String key = String.format("%02d_%s_%010d_%015d_%010d_%03d_%04d", 
        		hashRegion, blockID, (time / 3600) * 3600, imsi, time, timeMs, GetRandom());
        return key;
    }

    public static String DtLteCellGridKey(int randomMin, int randomMax, int lac, int ci, int time, int longitude, int latitude)
    {
        Random rand = new Random();
        String key = String.format("%06d_%08d_%010d_%010d_%09d_%04d", new Object[] {
            Integer.valueOf(lac), Integer.valueOf(ci), Integer.valueOf(time), Integer.valueOf(longitude), Integer.valueOf(latitude), Integer.valueOf(Math.abs(rand.nextInt() % 10000))
        });
        return key;
    }

    public static String DtLteGridKey(int randomMin, int randomMax, int time, int longitude, int latitude)
    {
        String blockID = GetBlockID(longitude, latitude);
        int hashRegion = GetRegion(randomMin, randomMax, time, blockID);
        
        String key = String.format("%02d_%s_%010d_%04d", 
        		hashRegion, blockID, (time / 3600) * 3600, GetRandom());
        return key;
    }

    public static String DtLteCellKey(int randomMin, int randomMax, long ci, int time)
    {
        String key = String.format("%010d_%010d_%04d", new Object[] {
            Long.valueOf(ci), Integer.valueOf(time), GetRandom()
        });
        return key;
    }

    public static String GetBlockID(int ilongitude, int ilatitude)
    {
    	//317500 + 61111
        return String.format("%06d%06d", ilongitude / 4000, ilatitude / 3600 );
        
    }

    public static String DtLteSampleIndexKey(int randomMin, int randomMax, long imsi, int time, int longitude, int latitude)
    {
    	String blockID = GetBlockID(longitude, latitude);
    	return DtLteSampleIndexKey(randomMin, randomMax, imsi, time, blockID);
    }

    public static String DtLteSampleIndexKey(int randomMin, int randomMax, long imsi, int time, String blockID)
    {
        int hashRegion = GetImsiRegion(randomMin, randomMax, time, imsi);
        
        String key = String.format("%02d_%015d_%010d_%s", 
        		hashRegion, imsi, (time / 3600) * 3600, blockID);
        return key;
    }

    public static String DtLteEventIndexKey(int randomMin, int randomMax, long imsi, int time, int longitude, int latitude)
    {
    	String blockID = GetBlockID(longitude, latitude);
    	return DtLteSampleIndexKey(randomMin, randomMax, imsi, time, blockID);
    }

    public static String DtLteEventIndexKey(int randomMin, int randomMax, long imsi, int time, String blockID)
    {
        int hashRegion = GetImsiRegion(randomMin, randomMax, time, imsi);
        
        String key = String.format("%02d_%015d_%010d_%s", 
        		hashRegion, imsi, (time / 3600) * 3600, blockID);
        return key;
    }
    
    public static int GetDayTime(int tmTime)
    {
    	return (tmTime + 8*3600)/86400*86400-8*3600;
    }
    
    public static int GetRegion(int randomMin, int randomMax, int time, int longtitude, int longitude)
    {
    	int dayTime = GetDayTime(time);
    	String blockID = GetBlockID(longtitude, longitude);
    	return (blockID + "_" + dayTime).hashCode()%(randomMax-randomMin);
    }
    
    public static int GetRegion(int randomMin, int randomMax, int time, String blockID)
    {
    	int dayTime = GetDayTime(time);
    	return Math.abs((blockID + "_" + dayTime).hashCode()%(randomMax-randomMin));
    }
    
    public static int GetImsiRegion(int randomMin, int randomMax, int time, long imsi)
    {
    	int dayTime = GetDayTime(time);
    	return Math.abs((imsi + "_" + dayTime).hashCode()%(randomMax-randomMin));
    }
    
    public static int GetRandom()
    {
    	Random rand = new Random();
    	return Math.abs(rand.nextInt() % 10000);
    }
	
}
