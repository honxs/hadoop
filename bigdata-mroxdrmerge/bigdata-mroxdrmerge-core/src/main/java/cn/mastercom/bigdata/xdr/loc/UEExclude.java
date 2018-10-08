package cn.mastercom.bigdata.xdr.loc;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cn.mastercom.bigdata.util.DataGeter;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

public class UEExclude
{
	private Map<Long, Integer> imsiMap;
	
    private static UEExclude instance;
    public static UEExclude GetInstance()
    {
    	if(instance == null)
    	{
    		instance = new UEExclude();
    	}
    	return instance;
    }
	
    private UEExclude()
    {
    	imsiMap = new HashMap<Long, Integer>();
    }
    
    public boolean loadData() 
    {
    	try
		{
    		//xsh
    		Configuration conf = new Configuration();
    		HDFSOper hdfsOper = new HDFSOper(conf);
//    		HDFSOper hdfsOper = new HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(), MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());
    	 	String filePath = "/mt_wlyh/Data/config/tb_cfg_ue_exclude.txt";
    	 	if(!hdfsOper.checkFileExist(filePath))
    	 	{
    	 		return false;
    	 	}
        	
			BufferedReader reader = null;
			imsiMap = new HashMap<Long, Integer>();
			try
			{
				reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
				String strData;
				long imsi;
				while((strData = reader.readLine()) != null)
				{
					if(strData.length() == 0)
					{
						continue;
					}
					
					try
					{
						imsi = DataGeter.GetLong(strData);
						if(!imsiMap.containsKey(imsi))
						{
							imsiMap.put(imsi, 0);
						}
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "load ue exclued error","load ue exclued error : " + strData, e);
						return false;
					}		
				}
			}
			catch(Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "load ue exclued error","load ue exclued error ", e);
				return false;
			}
			finally
			{
				if (reader != null)
				{
					reader.close();
				}
			}
    	 	
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc UEExclude.loadData error",
					"xdrloc UEExclude.loadData error",	e);
			return false;
		}
   	
    	return true;
    }
    
    public boolean isUEExclude(long imsi)
    {
    	if(imsiMap.containsKey(imsi))
    	{
    		return true;
    	}
    	return false;
    }
    
    public void addImsi(long imsi)
    {
    	imsiMap.put(imsi, 0);
    }
    
}
