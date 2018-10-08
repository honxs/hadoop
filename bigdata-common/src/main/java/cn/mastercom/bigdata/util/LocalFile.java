package cn.mastercom.bigdata.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;


public class LocalFile
{
	private static Logger log = Logger.getLogger(LocalFile.class.getName());
	public static long CheckSpace(String sPath)
	{
		File win = new File(sPath);
		if (!win.exists())
			return 0;
		if (win.isFile())
			win = win.getParentFile();
		return win.getUsableSpace();
	}

	public static boolean deleteFile(String sPath)
	{
		boolean flag = false;
		File file = new File(sPath);
		if (file.isFile() && file.exists())
		{
			file.delete();
			flag = true;
		} 

		return !file.exists();
	}
	
	public static boolean isFileSizeChanging(String sPath, int checkSeconds)
	{
		boolean flag = false;
		File file = new File(sPath);
		if (file.isFile() && file.exists())
		{
			try {
				long size1=file.length();
				Thread.sleep(checkSeconds*1000);
				long size2=file.length();
				if(size2>size1)
				{
					flag = true;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} 
		return flag;
	}

	public static boolean  MergeFile(String localDirname, String destPath, String destFileName, String filter,boolean Compress )
	{
		try
		{
			File dir = new File(localDirname);
			if (!dir.isDirectory())
			{
				log.info(localDirname + " is not dir.");
				return false;
			}

			File[] files = dir.listFiles();
			if (files.length == 0)
				return false;

			log.info("Begin merge " + localDirname + " to " + destPath);

			if (checkFileExist(destPath + "/" + destFileName))
			{
				deleteFile(destPath + "/" + destFileName);
			}

			makeDir(destPath);

			List<String> listFile = new ArrayList<String>();
			for (int i = 0; i < files.length; i++) 
			{
				listFile.add(files[i].getAbsolutePath());
			}
			mergeFile(listFile, destPath + "/" + destFileName, Compress);

			log.info("Success move " + localDirname + " to " + destPath);
			return true;
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}
	
	
	public static boolean deleteFile_xdr(String sPath)
	{
		boolean flag = false;
		File file = new File(sPath);
		if (file.isFile() && file.exists())
		{
			flag = file.delete();
		}
		return flag;
	}

	public static boolean checkLocakFileExist(String filename)
	{
		try
		{
			File f = new File(filename);
			return f.exists();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}

	public static boolean makeDir(String dirName)
	{
		File file = new File(dirName);
		if (!file.exists() && !file.isDirectory())
		{
			file.mkdirs();
		}
		return true;
	}
	
	/**
	 * 建文件
	 * @param name
	 * @return
	 * @throws IOException 
	 */
	public static boolean makeFile(String name) throws IOException {
		File file = new File(name);
		if(!file.exists()) {
			return file.createNewFile();		
		}
		return true;
	}

	public static String getFileName(String file)
	{
		File f = new File(file);
		String name = f.getName();
		int pos = name.lastIndexOf('.');
		if (pos >= 0)
		{
			return name.substring(0, pos);
		} else
		{
			return name;
		}
	}

	public static boolean renameFile(String oldname, String newname)
	{	 
		if (!oldname.equals(newname))
		{
			makeDir(new File(newname).getParent());
			File oldfile = new File(oldname);
			if(!oldfile.exists())
			{
				return false;
			}
			File newfile = new File(newname);
			if (newfile.exists())
			{
				newfile.delete();
			}

			return oldfile.renameTo(newfile);
		}
		return false;
	}

	public static boolean bkFile(String oldname, String newname)
	{
		if (!oldname.equals(newname))
		{
			makeDir(new File(newname).getParent());
			File oldfile = new File(oldname);
			File newfile = new File(newname);
			if (newfile.exists())
			{
				newfile.delete();
			}
			return oldfile.renameTo(newfile);
		} else
		{
			return false;
		}
	}

	public static void renameDirectory(String fromDir, String toDir)
	{

		File from = new File(fromDir);

		if (!from.exists() || !from.isDirectory())
		{
			log.info("Directory does not exist: " + fromDir);
			return;
		}

		File to = new File(toDir);

		// Rename
		if (from.renameTo(to))
			log.info( from + " rename Success!");
		else
			log.info( from + " rename Error");

	}

	public String mergeFile(String localDirname,String outputLocalName,boolean Compress)
	{
		try
		{						
			File dir = new File(localDirname);
			if (!dir.isDirectory())
			{
				log.info(localDirname + "不是目录 ");
				return "";
			}

			File[] files = dir.listFiles();
			if (files.length == 0)
				return "";

			List<String> listFile = new ArrayList<String>();
			for (int i = 0; i < files.length; i++) 
			{
				listFile.add(files[i].getAbsolutePath());
			}
			return mergeFile(listFile, outputLocalName,Compress);
		}
		catch (Exception e)
		{
			log.error("mergeFile error " + e.getMessage());
		}
		return "";
	}
	
	public static String mergeFile(List<String> fileLst,String outputLocalName,boolean Compress)
	{
		if (fileLst != null)
		{
			try
			{
				FileOutputStream os = null;
				GZIPOutputStream gfs = null;
				FileInputStream is = null;
				GZIPInputStream gis = null;
				
				int fileSeq = 1;
				long nTotalLen = 0;
				String fileName = outputLocalName;

				if (Compress)
				{
					gfs = new GZIPOutputStream(new FileOutputStream(fileName + ".gz"));
					os = null;
				}
				else
				{
					os = new FileOutputStream(fileName);
					gfs = null;
				}

				for (int i = 0; i < fileLst.size(); i++)
				{					
					File file = new File(fileLst.get(i));
					if (!file.exists())
						continue;
					is = new FileInputStream(file);
					gis = null;
					if (file.getName().toLowerCase().endsWith("gz"))
						gis = new GZIPInputStream(is);
					
					byte[] buffer = new byte[1024000];
					int length = 0;
					while (true)
					{
						if (gis == null)
							length = is.read(buffer);
						else
							length = gis.read(buffer, 0, buffer.length);
						
						if(length<0)
							break;
						
						if (gfs != null)
						{
							gfs.write(buffer, 0, length);
						}
						else
						{
							os.write(buffer, 0, length);
						}

					}				

					if (gis != null)
						gis.close();					
					is.close();
				}

				if (gfs != null)
				{
					gfs.finish();
					gfs.close();
				}
				else
				{
					os.flush();
					os.close();
				}

			}
			catch (Exception e)
			{
				log.info(" Task Exec Error:" + outputLocalName + "\r\n" + e.getMessage());
				return "Task Exec Error:" + outputLocalName + "\r\n" + e.getMessage();
			}
			finally
			{

			}
		}
		return "Task Exec Success:" + outputLocalName;
		
	}
	
	public static void main(String[] args)
	{
		if(args.length>=4 && args[0].equals("-mv"))
		{
			MoveFile(args[1],args[2],args[3]);
		}
		//LocalFile.renameDirectory("D:/test/output", "D:/test/mro/output");
		//LocalFile.isFileSizeChanging("g:/jwd.txt",2);
		//System.out.println(LocalFile.deleteFile(args[0]));
		/*log.info("102989452115436299".hashCode());
		try {
			List<String> lst = LocalFile.getAllFiles(new File("d:/data/mr"),"MR",2);
			log.info(lst.size());
		} catch (Exception e) {
			e.printStackTrace();
		}*/
	}

	public final static List<String> getAllFiles(File dir, String filter, int waitMinute) throws Exception
	{
		//log.info("Scan Dir:"+dir.getAbsolutePath());

		List<String> fileList = new ArrayList<String>();
		if (!dir.exists())
			return fileList;
		File[] fs = dir.listFiles();

		for (int i = 0; i < fs.length; i++)
		{
			if (fs[i].isDirectory())
			{  
				try
				{
					fileList.addAll(getAllFiles(fs[i], filter, waitMinute));
				} catch (Exception e)
				{
					log.error("getAllFiles error:" + dir.getName() + ", " + e.getMessage());
				}
			}  
			else
			{
				if (waitMinute > 0)
				{
					long lastDirModifyTime = fs[i].lastModified() / 1000L;
					final CalendarEx cal = new CalendarEx(new Date());
					if (lastDirModifyTime + waitMinute * 60 > cal._second)
					{
						continue;
					}
				}
				boolean bFltResult = false;
				if(filter.length()>0)
				{
					bFltResult=false;
					String[] vct =filter.replace(",", " ").split(" ");
					for(String flt: vct)
					{
						if (!flt.equals("") && fs[i].getName().toLowerCase().contains(flt.toLowerCase()))
						{
							bFltResult = true;
							break;
						}
					}
				}
				else {
					bFltResult = true;
				}

				if(bFltResult == true)
				{
					fileList.add(fs[i].getAbsolutePath());
				}
			}
		}
		if(fileList.size()>0)
			log.info("Find "+dir.getAbsolutePath() + " files num:" + fileList.size());
		return fileList;
	}
	
	public final static void MoveFile(String src,String dst,String keyword)
	{
		if(!new File(src).exists())
			return;
		makeDir(dst);
		try {
			List<String> files = getAllFiles(new File(src), keyword, 1);
			for(String file:files)
			{
				moveFile(file,dst + "\\" + new File(file).getName());
				log.info("success move " + file + " to " + dst);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	public final static List<String> getPartFiles(File dir, String filter, int waitMinute,int MaxNum) throws Exception
	{
		List<String> fileList = new ArrayList<String>();
		if (!dir.exists())
			return fileList;
		File[] fs = dir.listFiles();

		for (int i = 0; i < fs.length; i++)
		{
			if(fileList.size()>=MaxNum )
				return fileList;
			if (fs[i].isDirectory())
			{  
				try
				{
					fileList.addAll(getPartFiles(fs[i], filter, waitMinute,MaxNum-fileList.size()));
				} catch (Exception e)
				{
					log.info("getAllFiles error:" + dir.getName() + ", " + e.getMessage());
				}
			}  
			else
			{
				if (waitMinute > 0)
				{
					long lastDirModifyTime = fs[i].lastModified() / 1000L;
					final CalendarEx cal = new CalendarEx(new Date());
					if (lastDirModifyTime + waitMinute * 60 > cal._second)
					{
						continue;
					}
				}
				boolean bFltResult = false;
				if(filter.length()>0)
				{
					bFltResult=false;
					String[] vct =filter.replace(",", " ").split(" ");
					for(String flt: vct)
					{
						if (!flt.equals("") && fs[i].getName().toLowerCase().contains(flt.toLowerCase()))
						{
							bFltResult = true;
							break;
						}
					}
				}

				if(bFltResult == true)
				{
					fileList.add(fs[i].getAbsolutePath());
				}
			}
		}
		if(fileList.size()>0)
			log.info("Find "+dir.getAbsolutePath() + " files num:" + fileList.size());
		return fileList;
	}
	
	/**
	 * 获取文件文件列表
	 * @param dir
	 * @param filter
	 * @param waitMinute
	 * @param nDepth 文件夹层数
	 * @param nMaxdirs 文件夹数据
	 * @return 
	 * @throws Exception
	 */
	public final static List<String> getAllDirs(File dir, String filter, int waitMinute,int nDepth, int nMaxdirs) throws Exception
	{
		List<String> fileList = new ArrayList<String>();
		if (!dir.exists())
			return fileList;
		
		File[] fs = dir.listFiles();
		Arrays.sort(fs,new FileComparator());

		for (int i = 0; i < fs.length; i++)
		{
			if(fileList.size()>nMaxdirs)
				return fileList;
			if (fs[i].isDirectory())
			{
				try
				{
					if(nDepth>0)
					{					
						fileList.addAll(getAllDirs(fs[i], filter, waitMinute,nDepth-1,nMaxdirs-fileList.size()));					
					}
					else if(nDepth==0 && fs[i].getName().equals("upload"))
					{
						fileList.addAll(getAllDirs(fs[i], filter, waitMinute,nDepth,nMaxdirs-fileList.size()));
					}
					else
					{
						fileList.add(fs[i].getAbsolutePath());
					}
				} catch (Exception e)
				{
					log.info("getAllFiles error:" + dir.getName() + ", " + e.getMessage());
				}
			}else {
				fileList.add(fs[i].getAbsolutePath());
			} 			
		}
		return fileList;
	}
	
	public final static List<String> getAllFiles(String files[], String filter, int waitMinute) throws Exception
	{
		List<String> fileList = new ArrayList<String>();
		for (String s : files)
		{
			File file = new File(s);
			fileList.addAll(getAllFiles(file, filter, waitMinute));
		}
		return fileList;
	}

	public final static List<String> getAllFiles(ArrayList<String> files, String filter, int waitMinute)
			throws Exception
	{
		List<String> fileList = new ArrayList<String>();
		for (String s : files)
		{
			File file = new File(s);
			fileList.addAll(getAllFiles(file, filter, waitMinute));
		}
		return fileList;
	}

	public static boolean deleteDirectory(String sPath)
	{
		
		if (!sPath.endsWith(File.separator))
		{
			sPath = sPath + File.separator;
		}
		File dirFile = new File(sPath);
		
		if (!dirFile.exists() || !dirFile.isDirectory())
		{
			return false;
		}
		boolean flag = true;
		
		File[] files = dirFile.listFiles();
		for (int i = 0; i < files.length; i++)
		{
			if (files[i].isFile())
			{
				flag = deleteFile(files[i].getAbsolutePath());
				if (!flag)
					break;
			} 
			else
			{
				flag = deleteDirectory(files[i].getAbsolutePath());
				if (!flag)
					break;
			}
		}
		if (!flag)
			return false;
		
		if (dirFile.delete())
		{
			return true;
		} else
		{
			return false;
		}
	}

	public static boolean checkFileExist(String filename)
	{
		try
		{
			File file = new File(filename);
			if (file.exists())
			{
				return true;
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}
	
	public static boolean checkFileExists(String filename)
	{
		try
		{
			File file = new File(filename);
			if (file.exists())
			{
				File[] listFiles = file.listFiles();
				for (File path : listFiles) {
					if(path.getName().equals("_SUCCESS")){
						return true;
					}
				}
				return false;
			}			
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}

	@SuppressWarnings("resource")
	private static void copyFileUsingFileChannels(File source, File dest) throws IOException {
		FileChannel inputChannel = null;
		FileChannel outputChannel = null;
		try {
			inputChannel = new FileInputStream(source).getChannel();
			outputChannel = new FileOutputStream(dest).getChannel();
			outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
		} finally {
			inputChannel.close();
			outputChannel.close();
		}
	}

	public static void moveFile(String oldname, String newname) throws IOException {
		if (!oldname.equals(newname)) {
			makeDir(new File(newname).getParent());
			File oldfile = new File(oldname);
			File newfile = new File(newname);
			if (newfile.exists()) {
				newfile.delete();
			}
			copyFileUsingFileChannels(oldfile, newfile);
			oldfile.delete();
		}
	}
	
	/**
	 * 移动指定文件或文件夹(包括所有文件和子文件夹)
	 * @param fromDir
	 *  要移动的文件或文件夹
	 * @param toDir
	 *  目标文件夹
	 * @throws Exception
	 */
	public static void MoveFolderAndFileWithSelf(String from, String to) throws Exception {
		try {
			File dir = new File(from);
			// 目标
			to +=  File.separator + dir.getName();
			File moveDir = new File(to);
			if(dir.isDirectory()){
				if (!moveDir.exists()) {
					moveDir.mkdirs();
				}
			}else{
				File tofile = new File(to);
				dir.renameTo(tofile);
				return;
			}			
			// 文件一览
			File[] files = dir.listFiles();
			if (files == null)
				return;
 
			// 文件移动
			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					MoveFolderAndFileWithSelf(files[i].getPath(), to);
					// 成功，删除原文件
					files[i].delete();
				}
				File moveFile = new File(moveDir.getPath() + File.separator + files[i].getName());
				// 目标文件夹下存在的话，删除
				if (moveFile.exists()) {
					moveFile.delete();
				}
				files[i].renameTo(moveFile);
			}
			dir.delete();
		} catch (Exception e) {
			throw e;
		}
	}
}

class FileComparator implements Comparator<File> {
	@Override
	public int compare(File o1, File o2) {
		return o1.getName().compareTo(o2.getName());
	}
}
