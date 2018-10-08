package cn.mastercom.bigdata.util.hadoop.schedule;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class JobInputMng
{
	//保存统计所需的数据源的jobInfo
	private Map<String, JobInputUnit> statInfoMap;
	
	public Map<String, JobInputUnit> getStatInfoMap()
	{
		return statInfoMap;
	}

	public JobInputMng(String jobInput) throws Exception
	{
		statInfoMap = new HashMap<String, JobInputUnit>();

		if (jobInput.length() > 0)
		{
			String tmpStr = new String(jobInput);
			tmpStr = tmpStr.substring(0, 0).equals("[") ? tmpStr.substring(1) : tmpStr;
			tmpStr = tmpStr.substring(tmpStr.length() - 1, tmpStr.length() - 1).equals("]")
					? tmpStr.substring(0, tmpStr.length() - 2) : tmpStr;

			String[] strSlipt = tmpStr.split("],[");
			for (String str : strSlipt)
			{
				String[] ss = str.split("|");
				JobInputItem jobInputItem = new JobInputItem();
				jobInputItem.fillData(ss, 0);
				
				String jobName = ComHelper.GetJobName(jobInputItem.jobType, jobInputItem.stime, jobInputItem.etime);
				JobInputUnit jobInputUnit = new JobInputUnit();
				jobInputUnit.jobInputItem = jobInputItem;
				statInfoMap.put(jobName, jobInputUnit);
			}
		}

	}
	
	
    public boolean checkInputInfoToStat(Map<String, JobInfo> curJobMap)
    {
        if(statInfoMap.size() == 0)
        {
        	return true;
        }
        
    	for(Map.Entry<String, JobInputUnit> jobInputUnitEntry : statInfoMap.entrySet())
    	{
    	   JobInfo jobInfo = curJobMap.get(jobInputUnitEntry.getKey()); 
    	   jobInputUnitEntry.getValue().jobInfo = jobInfo;
    	}
    	
    	boolean checkRight = true;
    	for(Map.Entry<String, JobInputUnit> statInfoMapEntry : statInfoMap.entrySet())
    	{
			if(!statInfoMapEntry.getValue().checkInputUnitOkToStat())
			{
				checkRight = false;
				break;
			}
		}
    	
        if(!checkRight)
        {
        	return false;
        }
    	return true;
    	
    }
    
    public boolean checkInputInfoReStat(Map<String, JobInfo> curJobMap)
    {
        if(statInfoMap.size() == 0)
        {
        	return false;
        }
        
    	for(Map.Entry<String, JobInputUnit> jobInputUnitEntry : statInfoMap.entrySet())
    	{
    	   JobInfo jobInfo = curJobMap.get(jobInputUnitEntry.getKey());
    	   jobInputUnitEntry.getValue().jobInfo = jobInfo;
    	}
    	
    	boolean checkRight = false;
    	for(Map.Entry<String, JobInputUnit> statInfoMapEntry : statInfoMap.entrySet())
    	{
    		int result = statInfoMapEntry.getValue().checkInputUnitOkReStat();
    		if(result < 0)
    		{
    			checkRight = false;
    			break;
    		}
    		else if(result > 0)
    		{
    			checkRight = true;
    		}
		}
    	
        if(!checkRight)
        {
        	return false;
        }
    	return true;
    	
    }
    
    
    public class JobInputUnit
    {
    	public JobInputItem jobInputItem;
    	public JobInfo jobInfo;
    	
    	public boolean checkInputUnitOkToStat()
    	{
    		if(jobInfo == null)
    		{
    			return false;
    		}
    		else 
    		{
        		if(jobInfo.status == EJobStatus.Success)
        		{
        			return true;
        		}
        		return false;
    		}

    	}
    	
    	public int checkInputUnitOkReStat()
    	{
    		if(jobInfo == null)
    		{
    			return -1;
    		}
    		
    		if(jobInfo.status != EJobStatus.Success)
    		{
    			return -1;
    		}
    		
			if(jobInputItem.finTime.getTime() < jobInfo.finTime.getTime())
			{
				return 1;
			}
			
			return 0;
    	}
  
    }

	public class JobInputItem
	{
		public String jobType;
		public Date stime;
		public Date etime;
		public Date finTime;

		public void fillData(String[] ss, int startPos) throws ParseException
		{
			jobType = ss[startPos++];
			stime = ComHelper.dateFormat.parse(ss[startPos++]);
			etime = ComHelper.dateFormat.parse(ss[startPos++]);
			finTime = ComHelper.dateFormat.parse(ss[startPos++]);
		}
	}

}
