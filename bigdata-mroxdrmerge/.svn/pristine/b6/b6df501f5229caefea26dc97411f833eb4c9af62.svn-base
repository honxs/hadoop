package com.chinamobile.util;


import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LocalFile
{
	public static long CheckSpace(String sPath) {        
        File win = new File(sPath);  
        System.out.println("Free space = " + win.getUsableSpace());  
        return win.getUsableSpace();  
    }  
	
	public static boolean deleteFile(String sPath) {  
	    boolean flag = false;  
	    File file = new File(sPath);  
	    // 璺緞涓烘枃浠朵笖涓嶄负绌哄垯杩涜鍒犻櫎  
	    if (file.isFile() && file.exists()) {  
	        file.delete();  
	        flag = true;  
	    }  
	    else if(!file.exists())
	    {
	    	flag = true;
	    }
	    return flag;  
	}
	
	public static boolean checkLocakFileExist(String filename)
	{
		try
		{
			File f = new File(filename);
			return f.exists();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}
	
	public static boolean makeDir(String dirName)
	{
		File file =new File(dirName);    
		if  (!file .exists()  && !file .isDirectory())      
		{       
		    file.mkdirs();    
		}  
		return true;
	}
	
	public static void renameFile(String oldname,String newname){
        if(!oldname.equals(newname))
        {
        	makeDir(new File(newname).getParent());
            File oldfile=new File(oldname);
            File newfile=new File(newname);
            if(newfile.exists())
            {
            	newfile.delete();
            }
            
            oldfile.renameTo(newfile);
        }         
    }
	
	public final static List<String> getAllFiles(File dir, String filter, int waitMinute) throws Exception
	 {
		  File[] fs = dir.listFiles();
		  List<String> fileList = new  ArrayList<String>();
		  for(int i=0; i<fs.length; i++)
		  {
			  if(fs[i].isDirectory())
			  {
				  try{
					  fileList.addAll(getAllFiles(fs[i],filter,waitMinute));
				  }
				  catch(Exception e)
				  {}
			  }
			  else
			  {
				  if(waitMinute>0)
				  {
					  long lastDirModifyTime = fs[i].lastModified()/1000L;
					  final CalendarEx cal = new CalendarEx(new Date());
					  if(lastDirModifyTime + waitMinute >cal._second)
					  {
						  continue;
					  }
				  }			  
					
				  if(filter.equals("") || fs[i].getName().contains(filter))
					  fileList.add(fs[i].getAbsolutePath());
			  }
		  }
		  return fileList;
	 }
	
	/**
     * 鍒犻櫎鐩綍锛堟枃浠跺す锛変互鍙婄洰褰曚笅鐨勬枃浠�
     * @param   sPath 琚垹闄ょ洰褰曠殑鏂囦欢璺緞
     * @return  鐩綍鍒犻櫎鎴愬姛杩斿洖true锛屽惁鍒欒繑鍥瀎alse
     */
    public static boolean deleteDirectory(String sPath) {
        //濡傛灉sPath涓嶄互鏂囦欢鍒嗛殧绗︾粨灏撅紝鑷姩娣诲姞鏂囦欢鍒嗛殧绗�
        if (!sPath.endsWith(File.separator)) {
            sPath = sPath + File.separator;
        }
        File dirFile = new File(sPath);
        //濡傛灉dir瀵瑰簲鐨勬枃浠朵笉瀛樺湪锛屾垨鑰呬笉鏄竴涓洰褰曪紝鍒欓��鍑�
        if (!dirFile.exists() || !dirFile.isDirectory()) {
            return false;
        }
        boolean flag = true;
        //鍒犻櫎鏂囦欢澶逛笅鐨勬墍鏈夋枃浠�(鍖呮嫭瀛愮洰褰�)
        File[] files = dirFile.listFiles();
        for (int i = 0; i < files.length; i++) {
            //鍒犻櫎瀛愭枃浠�
            if (files[i].isFile()) {
                flag = deleteFile(files[i].getAbsolutePath());
                if (!flag) break;
            } //鍒犻櫎瀛愮洰褰�
            else {
                flag = deleteDirectory(files[i].getAbsolutePath());
                if (!flag) break;
            }
        }
        if (!flag) return false;
        //鍒犻櫎褰撳墠鐩綍
        if (dirFile.delete()) {
            return true;
        } else {
            return false;
        }
    }
    
	public static boolean checkFileExist(String filename)
	{
		try
		{
		    File file = new File(filename);  
		    if (file.exists()) {  
			    return true;
		    }  
		}
		catch (Exception e)
		{
			e.printStackTrace(); 
		}
		return false;
	}
	
}
