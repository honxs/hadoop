package com.chinamobile.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GZipUtil
{

    private static Logger log = LoggerFactory.getLogger(GZipUtil.class);

    public static boolean doCompressFile(String inFileName)
    {
        try
        {
            String outFileName = inFileName + ".gz";
            GZIPOutputStream out = null;
            out = new GZIPOutputStream(new FileOutputStream(outFileName));
            FileInputStream in = null;
            in = new FileInputStream(inFileName);
            byte[] buf = new byte[1024];
            int len;
            while ((len = in.read(buf)) > 0)
            {
                out.write(buf, 0, len);
            }
            in.close();
            out.finish();
            out.close();
            return true;

        } catch (IOException e)
        {
            log.error("Compression from " + inFileName + "failed.");
            e.printStackTrace();
            return false;
        }
    }

    public static boolean doUncompressFile(String inFileName)
    {

        try
        {

            if (!getExtension(inFileName).equalsIgnoreCase("gz"))
            {
                log.error(inFileName + "is not a .gz file!");
                return false;
            }
            FileInputStream fis = new FileInputStream(inFileName);
            GZIPInputStream in = null;
            try
            {
                in = new GZIPInputStream(fis);
            } catch (Exception e)
            {
                log.error(inFileName + " not found or file is zero size. "
                        + inFileName);
                if (fis != null)
                {
                    fis.close();
                }
                return false;
            }
            String outFileName = getFileName(inFileName);
            FileOutputStream out = null;
            try
            {
                out = new FileOutputStream(outFileName);
            } catch (FileNotFoundException e)
            {
                log.error(inFileName + " could not be written to " + outFileName);
                if (in != null)
                {
                    in.close();
                }
                return false;
            }
            byte[] buf = new byte[1024];
            int len;
            if (in != null)
            {
                while ((len = in.read(buf)) > 0)
                {
                    out.write(buf, 0, len);
                }
            }
            if (in != null)
            {
                in.close();
            }
            if (out != null)
            {
                out.close();
            }

            return true;

        } catch (Exception e)
        {
            e.printStackTrace();
            return false;
        }

    }

    public static boolean EncryptGzip(String inFileName, String outFileName, byte key)
    {
        try
        {
            GZIPOutputStream out = null;
            out = new GZIPOutputStream(new FileOutputStream(outFileName));
            FileInputStream in = null;
            in = new FileInputStream(inFileName);
            byte[] buf = new byte[1024];
            int len;
            while ((len = in.read(buf)) > 0)
            {
                out.write(xorBytes(buf, (byte) key), 0, len);
            }
            in.close();
            out.finish();
            out.close();
            return true;

        } catch (IOException e)
        {
            log.error("Compression from " + inFileName + "failed.");
            e.printStackTrace();
            return false;
        }
    }

    /*public static boolean EncryptGzipBase64(String inFileName, String outFileName)
    {
        GZIPOutputStream out = null;
        FileInputStream in = null;
        BufferedReader dr = null;
        boolean zipSuccess = true;
        try
        {
            out = new GZIPOutputStream(new FileOutputStream(outFileName));
            in = new FileInputStream(inFileName);
            dr = new BufferedReader(new InputStreamReader(in));
            String str = null;
            while ((str = dr.readLine()) != null)
            {
                byte[] dataBytes = str.getBytes();
                out.write(new BASE64Encoder().encode(dataBytes).getBytes());
                out.write("\n".getBytes());
            }

        } catch (IOException e)
        {
            log.error("Compression from " + inFileName + "failed.");
            e.printStackTrace();
            zipSuccess = false;
        } finally
        {
            try
            {
                dr.close();
                in.close();
                out.finish();
                out.close();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
            return zipSuccess;
        }
    }

    public static boolean DecryptGzipBase64(String inFileName)
    {
        try
        {
            if (!getExtension(inFileName).equalsIgnoreCase("gz"))
            {
                log.error(inFileName + "is not a .gz file!");
                return false;
            }
            FileInputStream fis = new FileInputStream(inFileName);
            GZIPInputStream in = null;
            try
            {
                in = new GZIPInputStream(fis);
            } catch (Exception e)
            {
                log.error(inFileName + " not found or file is zero size. "
                        + inFileName);
                if (fis != null)
                {
                    fis.close();
                }
                return false;
            }
            String outFileName = getFileName(inFileName);
            FileOutputStream out = null;
            try
            {
                out = new FileOutputStream(outFileName);
            } catch (FileNotFoundException e)
            {
                log.error(inFileName + " could not be written to " + outFileName);
                if (in != null)
                {
                    in.close();
                }
                return false;
            }
            BufferedReader dr = null;
            if (in != null)
            {
                dr = new BufferedReader(new InputStreamReader(in));
                String str = null;
                while ((str = dr.readLine()) != null)
                {
                    byte[] b = new BASE64Decoder().decodeBuffer(str);
                    out.write(b);
                    out.write("\n".getBytes());
                }
            }
            if (dr != null)
            {
                dr.close();
            }
            if (in != null)
            {
                in.close();
            }
            if (out != null)
            {
                out.close();
            }

            return true;

        } catch (Exception e)
        {
            e.printStackTrace();
            return false;
        }

    }*/

    public static boolean DecryptGzip(String inFileName, byte key)
    {
        try
        {
            if (!getExtension(inFileName).equalsIgnoreCase("gz"))
            {
                log.error(inFileName + "is not a .gz file!");
                return false;
            }
            FileInputStream fis = new FileInputStream(inFileName);
            GZIPInputStream in = null;
            try
            {
                in = new GZIPInputStream(fis);
            } catch (Exception e)
            {
                log.error(inFileName + " not found or file is zero size. "
                        + inFileName);
                if (fis != null)
                {
                    fis.close();
                }
                return false;
            }
            String outFileName = getFileName(inFileName);
            FileOutputStream out = null;
            try
            {
                out = new FileOutputStream(outFileName);
            } catch (FileNotFoundException e)
            {
                log.error(inFileName + " could not be written to " + outFileName);
                if (in != null)
                {
                    in.close();
                }
                return false;
            }
            byte[] buf = new byte[1024];
            int len;
            if (in != null)
            {
                while ((len = in.read(buf)) > 0)
                {
                    out.write(xorBytes(buf, (byte) key), 0, len);
                }
            }
            if (in != null)
            {
                in.close();
            }
            if (out != null)
            {
                out.close();
            }

            return true;

        } catch (Exception e)
        {
            e.printStackTrace();
            return false;
        }

    }

    public static String doUncompressXMLFileWithSameName(String inFileName, String tempPath)
    {

        String outFileName = null;
        try
        {
            if (!getExtension(inFileName).equalsIgnoreCase("gz"))
            {
                log.error(inFileName + "is not a .gz file!");
                return null;
            }
            FileInputStream fis = new FileInputStream(inFileName);
            GZIPInputStream in = null;
            try
            {
                in = new GZIPInputStream(fis);
            } catch (Exception e)
            {
                log.error(inFileName + " not found or file is zero size. "
                        + inFileName);
                if (fis != null)
                {
                    fis.close();
                }
                return null;
            }

            if (tempPath.endsWith(File.separator))
            {
                tempPath = tempPath.substring(0, tempPath.length() - 1);
            }
            String uncompressedFileName = tempPath + inFileName;
            outFileName = uncompressedFileName.replace(".gz", "");
            FileUtil.checkDirExistsAndMkDir(new File(outFileName.substring(0, outFileName.lastIndexOf(File.separator))));

            FileOutputStream out = null;
            try
            {
                out = new FileOutputStream(outFileName);
            } catch (FileNotFoundException e)
            {
                log.error(inFileName + " could not be written to " + outFileName);
                if (in != null)
                {
                    in.close();
                }
                return null;
            }
            byte[] buf = new byte[1024];
            int len;
            if (in != null)
            {
                while ((len = in.read(buf)) > 0)
                {
                    out.write(buf, 0, len);
                }
            }
            if (in != null)
            {
                in.close();
            }
            if (out != null)
            {
                out.close();
            }

            return outFileName;

        } catch (Exception e)
        {
            System.out.println("inFileName:" + inFileName);
            System.out.println("outFileName:" + outFileName);
            System.out.println("tempPath:" + tempPath);
            e.printStackTrace();
            return null;
        }

    }

    public static String doUncompressXMLFileWithRandomName(String inFileName, String tempPath)
    {

        try
        {

            if (!getExtension(inFileName).equalsIgnoreCase("gz"))
            {
                log.error(inFileName + "is not a .gz file!");
                return null;
            }
            FileInputStream fis = new FileInputStream(inFileName);
            GZIPInputStream in = null;
            try
            {
                in = new GZIPInputStream(fis);
            } catch (Exception e)
            {
                log.error(inFileName + " not found or file is zero size. "
                        + inFileName);
                if (fis != null)
                {
                    fis.close();
                }
                return null;
            }
            String filePathName = "";
            int iii = inFileName.lastIndexOf(File.separator);
            if (iii > 0 && iii < inFileName.length())
            {
                filePathName = inFileName.substring(0, iii + 1);
                //change to temp file path for gzipped file
                // i.e. tempPath = "/abc/def/";
                filePathName = filePathName.replaceFirst(filePathName.substring(0, filePathName.indexOf(File.separator, 1) + 1), tempPath);
                FileUtil.checkDirExistsAndMkDir(new File(filePathName));
            }
            String randomFileName = TimeUtil.getTimeStringFromMillis(
                    System.currentTimeMillis(), "yyyyMMddHHmmss") + ".xml";

            String outFileName = filePathName + randomFileName;
            FileOutputStream out = null;
            try
            {
                out = new FileOutputStream(outFileName);
            } catch (FileNotFoundException e)
            {
                log.error(inFileName + " could not be written to " + outFileName);
                if (in != null)
                {
                    in.close();
                }
                return null;
            }
            byte[] buf = new byte[1024];
            int len;
            if (in != null)
            {
                while ((len = in.read(buf)) > 0)
                {
                    out.write(buf, 0, len);
                }
            }
            if (in != null)
            {
                in.close();
            }
            if (out != null)
            {
                out.close();
            }

            return outFileName;

        } catch (Exception e)
        {
            e.printStackTrace();
            return null;
        }

    }

    public static String getExtension(String f)
    {
        String ext = "";
        int i = f.lastIndexOf('.');

        if (i > 0 && i < f.length() - 1)
        {
            ext = f.substring(i + 1);
        }
        return ext;
    }

    public static String getFileName(String f)
    {
        String fname = "";
        int i = f.lastIndexOf('.');

        if (i > 0 && i < f.length() - 1)
        {
            fname = f.substring(0, i);
        }
        return fname;
    }


    /**
     * XOR
     */
    public static byte[] xorBytes(byte[] originalBytes, byte keyByte)
    {
        byte[] encryptedBytes = new byte[originalBytes.length];

        for (int i = 0; i < originalBytes.length; i++)
        {
            encryptedBytes[i] = (byte) (originalBytes[i] ^ keyByte);
        }

        return encryptedBytes;
    }
}
