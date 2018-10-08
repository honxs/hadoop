package com.chinamobile.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;


/**
 * @author Zhou Xingwei
 */

public class FileUtil
{
    //private static Logger log = LoggerFactory.getLogger(FileUtil.class);

    /**
     获取目录下所有文件的文件名集合，不嵌套
     */

    public static HashSet<String> getShortFileNameSet(String filePath)
    {
        File md = new File(filePath);
        if(!md.exists())
        {
            System.out.println("Directory" + filePath+ " does not exist so exit.");
            return null;
        }

        HashSet<String> resultSet = new HashSet<String>();
        File[] enbF = md.listFiles();
        if (enbF == null || enbF.length == 0)
        {
            System.out.println(filePath + " has no file");
            return null;
        }
        for(File tmpE : enbF)
        {
            resultSet.add(tmpE.getName());
        }

        return resultSet;
    }


    public static ArrayList<String> getFiles(String filePath)
    {
        ArrayList<String> filelist = new ArrayList<String>();
        filelist = getDirectoryFiles(filePath);
        return filelist;
    }

    private static ArrayList<String> getDirectoryFiles(String filePath)
    {
        ArrayList<String> filelist = new ArrayList<String>();
        File root = new File(filePath);
        File[] files = root.listFiles();
        for (File file : files)
        {
            //log.info(file.getAbsolutePath());
            if (file.isFile())
            {
                filelist.add(file.getAbsolutePath());
            } else
            {
                filelist.addAll(getDirectoryFiles(file.getAbsolutePath()));
            }
        }
        return filelist;
    }

    public synchronized static boolean checkDirExistsAndMkDir(File dir)
    {
        if (dir == null)
        {
            return false;
        }
        if (!dir.exists())
        {
            checkDirExistsAndMkDir(dir.getParentFile());
            dir.mkdir();
        }
        if (dir.exists() && dir.isDirectory())
        {
            return true;
        } else
        {
            return false;
        }

    }

    public static boolean WriteRandomAccessFile(String fileName,
                                                String lineString)
    {

        try
        {
            RandomAccessFile randomFile = new RandomAccessFile(fileName, "rw");
            long fileLength = randomFile.length();
            randomFile.seek(fileLength);
            String newline = lineString + "\r\n";
            randomFile.write(newline.getBytes("GBK"));
            randomFile.close();
            return true;
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        }

    }

    public static void CombineFiles(String fileDirectory)
    {
        if (!fileDirectory.endsWith(File.separator))
        {
            fileDirectory += File.separator;
        }
        File directory = new File(fileDirectory);
        File[] files = directory.listFiles();
        BufferedWriter bufferedWriter = null;
        try
        {
            bufferedWriter = new BufferedWriter(new FileWriter(fileDirectory + "combined.txt", true));
            System.out.println(fileDirectory + "combined.txt");
        } catch (IOException e)
        {
            e.printStackTrace();
        }

        for (File file : files)
        {
            BufferedReader reader = null;
            try
            {
                reader = new BufferedReader(new FileReader(file));
                String tempString = null;
                while ((tempString = reader.readLine()) != null)
                {
                    bufferedWriter.write(tempString + "\n");
                }
            } catch (IOException e)
            {
                e.printStackTrace();
            } finally
            {
                if (reader != null)
                {
                    try
                    {
                        reader.close();
                    } catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }

        if (bufferedWriter != null)
        {
            try
            {
                bufferedWriter.close();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    public static ArrayList<String> ReadRandomAccessFile(String fileName)
    {
        ArrayList<String> templist = new ArrayList<String>();
        try
        {
            long lastReadSize = 0; // Can be used to implement reading from
            // middle of file
            File newFile = new File(fileName);
            RandomAccessFile raf = new RandomAccessFile(newFile, "r");
            raf.seek(lastReadSize);
            String tempstr = "";
            while ((tempstr = raf.readLine()) != null)
            {
                String str = new String(tempstr.getBytes("ISO-8859-1"), "GBK");
                templist.add(str);
            }
            raf.close();
            return templist;
        } catch (IOException e)
        {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Separate one file into multiple files with same filesize
     *
     * @param fileName  文件绝对名
     * @param filecount 分多少份
     *                  缺陷：出现断行
     */
    public static void SeparateFilesWithFileSize(String fileName, int filecount)
    {
        File originalFile = new File(fileName);
        String filePath = originalFile.getParent();

        OutputStream os;
        try
        {
            RandomAccessFile raf = new RandomAccessFile(originalFile, "r");
            long totalFileSize = originalFile.length();
            int singleFileSize = (int) Math.ceil(totalFileSize / (double) filecount);

            for (int i = 0; i < filecount; i++)
            {
                long startPos = i * singleFileSize;
                byte[] b = new byte[singleFileSize];
                raf.seek(startPos);
                int s = raf.read(b);
                File file = new File(filePath + File.separator + i);
                FileOutputStream fop = new FileOutputStream(file);
                if (!file.exists())
                {
                    file.createNewFile();
                }
                fop.write(b, 0, s);
                fop.flush();
                fop.close();
            }
            System.out.println(originalFile.length() + ":" + singleFileSize);
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }


    /**
     * Separate one file into multiple files with same lines
     * @param fileName  文件绝对名
     * @param filecount 分多少份
     */
    public static void SeparateFilesWithLines(String fileName, int filecount)
    {
        File originalFile = new File(fileName);
        String filePath = originalFile.getParent();
        FileWriter[] fwList = new FileWriter[filecount];
        try
        {
            for (int i = 0; i < filecount; i++)
            {
                fwList[i] = new FileWriter(filePath + File.separator + i);
            }

            BufferedReader reader = null;
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "GBK"), 5 * 1024 * 1024);
            String line = null;
            int k = 0;
            while ((line = reader.readLine()) != null)
            {
                fwList[k].write(line+"\r\n");
                k++;
                if(k==filecount)
                {
                    k = 0;
                }
            }

        } catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            for (int i = 0; i < filecount; i++)
            {
                try
                {
                    fwList[i].close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }

        }


    }

    public static void CopyFile(String sourceFileName,
                                String targetFileName)
    {
        File sourceFile = new File(sourceFileName);

        String targetPath = targetFileName.substring(0,
                targetFileName.lastIndexOf(File.separatorChar) + 1);
        FileUtil.checkDirExistsAndMkDir(new File(targetPath));

        File targetFile = new File(targetFileName);
        try
        {
            targetFile.createNewFile();
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        InputStream in = null;
        OutputStream out = null;
        try
        {
            in = new BufferedInputStream(new FileInputStream(sourceFile));
            out = new BufferedOutputStream(new FileOutputStream(targetFile));
            byte[] buf = new byte[1024 * 1024];

            int b;
            while (true)
            {
                b = in.read(buf);
                if (b == -1)
                {
                    break;
                }
                out.write(buf, 0, b);
            }

            in.close();
            out.flush();
            out.close();

        } catch (Exception e)
        {// 捕获异常
            e.printStackTrace();
        } finally
        {
            if (in != null)
                try
                {
                    in.close();
                } catch (IOException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            if (out != null)
                try
                {
                    out.close();
                } catch (IOException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }
    }


    public static boolean WriteHashSetIntoFile(String fileName, HashSet<String> lines)
    {
        FileWriter fw = null;
        try
        {
            fw = new FileWriter(fileName, true);

            Iterator<String> iterator = lines.iterator();
            while(iterator.hasNext())
            {
                String line = iterator.next();
                fw.write(line+"\r\n");
            }
            return true;
        }
        catch(IOException e)
        {
            e.printStackTrace();
            return false;
        }
        finally
        {
            if(fw!=null)
            {
                try
                {
                    fw.close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String arg[])
    {
        String path = new File("Test").getAbsolutePath();
        System.out.println(path);
        ArrayList<String> filelist = getFiles(path);
        for (int i = 0; i < filelist.size(); i++)
        {
            System.out.println(filelist.get(i));
        }
        System.out.println(filelist.size());

    }
}
