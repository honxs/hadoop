package com.chinamobile.xdr;

import com.chinamobile.util.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class S1UCompressor
{
    private static String largeDirectory = "G:\\s1ularge22\\";
    private static String smallDirectory = "D:\\todelete\\";

    private static CountDownLatch mStartSignal;      //目前置为0，即立即开始
    private static CountDownLatch mDoneSignal;

    private static int TCOUNT = 1;

    long frameIndex = 0;

    public static void main(String[] args) throws Exception
    {

        ArgUtil.initArgs(args);
        String sourcedirectory = ArgUtil.getArg("sourcedirectory");
        if (sourcedirectory != null && sourcedirectory.length() > 0)
        {
            largeDirectory = sourcedirectory;
        }
        String targetdirectory = ArgUtil.getArg("targetdirectory");
        if (targetdirectory != null && targetdirectory.length() > 0)
        {
            smallDirectory = targetdirectory;
        }
        String threadNum = ArgUtil.getArg("thread");
        if(threadNum!=null && threadNum.length()>0)
        {
            TCOUNT = Integer.parseInt(threadNum);
        }

        List<String> pcapFileNameList = new ArrayList<String>();
        File ld = new File(largeDirectory);
        if (!ld.exists() || !ld.isDirectory())
        {
            System.out.println(largeDirectory + " does not exist or is not directory.");
            return;
        }

        File[] pcapfiles = ld.listFiles();
        if (pcapfiles != null && pcapfiles.length > 0)
        {
            for (File pcapfile : pcapfiles)
            {
                String fileName = pcapfile.getName();
                if (fileName.endsWith("cap") || fileName.endsWith("cap.gz"))
                {
                    pcapFileNameList.add(pcapfile.getName());
                    //System.out.println(absoluteFileName);
                } else
                {
                    System.out.println(fileName + " is not a pcap file.");
                }
            }
        }

        System.out.println(pcapFileNameList.size() + " pcap files to process under directory " + largeDirectory);

        File sd = new File(smallDirectory);
        FileUtil.checkDirExistsAndMkDir(sd);

        mStartSignal = new CountDownLatch(0);      //目前置为0，即立即开始
        mDoneSignal = new CountDownLatch(TCOUNT);

        if (pcapFileNameList != null && pcapFileNameList.size() > 0)
        {
            long startMill = System.currentTimeMillis();

            for (int i = 0; i < TCOUNT; i++)
            {
                List<String> tmpList = new ArrayList<String>();
                for (int j = 0; j < pcapFileNameList.size(); j++)
                {
                    if (j % TCOUNT == i)
                    {
                        tmpList.add(pcapFileNameList.get(j));
                    }
                }
                new Thread(new CompressThread(tmpList, mStartSignal, mDoneSignal, i)).start();
            }

            mDoneSignal.await();
            long endMill = System.currentTimeMillis();
            long timeUsed = endMill - startMill;
            System.out.println(pcapFileNameList.size() + " pcap files filtering finished with " + timeUsed + " ms used.");

        }

    }


    static class CompressThread implements Runnable
    {

        private List<String> fileList;
        private final CountDownLatch mStartSignal;
        private final CountDownLatch mDoneSignal;
        private final int mThreadIndex;

        public CompressThread(List<String> fileList, final CountDownLatch startSignal, final CountDownLatch doneSignal, final int threadIndex)
        {
            this.fileList = fileList;
            this.mDoneSignal = doneSignal;
            this.mStartSignal = startSignal;
            this.mThreadIndex = threadIndex;
        }

        @Override
        public void run()
        {
            try
            {
                if (fileList == null || fileList.size() == 0)
                {
                    return;
                }

                for (String tmpFile : fileList)
                {
                    System.out.println("Start to process: " + tmpFile);
                    long startMill = System.currentTimeMillis();
                    //抽取
                    String pcapName = tmpFile;

                    if (tmpFile.endsWith("cap.gz"))
                    {
                        boolean unzipStatus = GZipUtil.doUncompressFile(largeDirectory + tmpFile);

                        if (unzipStatus)
                        {
                            System.out.println(tmpFile + " successfully unzipped");
                            pcapName = tmpFile.substring(0, tmpFile.length() - 3);
                        } else
                        {
                            System.out.println(pcapName + ".pcap" + ".gz unzip failed");
                            continue;
                        }
                    }

                    File filteredFile = new File(smallDirectory + pcapName + ".pcap");
                    File gzfile = new File(smallDirectory + pcapName + ".pcap.gz");
                    if (gzfile.exists())
                    {
                        System.out.println(smallDirectory + pcapName + ".pcap.gz" + " already exists so skipped.");
                        continue;
                    }
                    FileOutputStream fos = new FileOutputStream(filteredFile);
                    S1UCompressor pcapCompressor = new S1UCompressor();
                    try
                    {
                        pcapCompressor.parse(fos, pcapName);
                    } catch (Exception e)
                    {
                        e.printStackTrace();
                    } finally
                    {
                        if (fos != null)
                        {
                            fos.close();
                        }

                        if (tmpFile.endsWith("cap.gz"))
                        {
                            File todel = new File(largeDirectory + pcapName);
                            todel.delete();
                        }
                    }

                    //压缩
                    boolean zipStatus = GZipUtil.doCompressFile(smallDirectory + pcapName + ".pcap");
                    if (zipStatus)
                    {
                        File targetFile = new File(smallDirectory + pcapName + ".pcap");
                        targetFile.delete();
                    }

                    long endMill = System.currentTimeMillis();
                    System.out.println("Finish processing: " + tmpFile+" "+(endMill-startMill)+" ms used.");
                }
            } catch (Exception e)
            {
                e.printStackTrace();
            } finally
            {
                mDoneSignal.countDown();// 完成以后计数减一
            }


        }
    }


    public void parse(FileOutputStream fos, String pcapName) throws Exception
    {
        InputStream inStream = null;
        BufferedInputStream bis = null;
        inStream = new FileInputStream(largeDirectory + pcapName);
        bis = new BufferedInputStream(inStream);

        byte[] pcapStartBytes = new byte[4];
        bis.read(pcapStartBytes);

        fos.write(pcapStartBytes);

        if ((pcapStartBytes[0] & 0xFF) != 0xD4 || (pcapStartBytes[1] & 0xFF) != 0xC3 || (pcapStartBytes[2] & 0xFF) != 0xB2 || (pcapStartBytes[3] & 0xFF) != 0xA1)
        {
            System.out.println(pcapName + " is not a valid file (not starting with D4C3B2A1)");
            return;
        }

        byte[] buf = new byte[20];
        bis.read(buf);
        fos.write(buf);

        byte[] bufMill = new byte[4];

        while (bis.read(bufMill) != -1)
        {
            ++frameIndex;
            boolean isValid = false;
            byte[] bufLen = new byte[4];
            bis.read(bufLen);
            byte[] rawLen = new byte[4];
            bis.read(rawLen);
            byte[] dropLen = new byte[4];
            bis.read(dropLen);

            int ethernetFrameLength = (int) StringUtil.reverseByte2Long(rawLen);
            if (ethernetFrameLength == 0)
            {
                continue;
            }
            byte[] currentBytes = new byte[ethernetFrameLength];
            bis.read(currentBytes);
            byte[] myBytes = currentBytes;

            //Ethernet II Header Length = 14
            if (currentBytes.length <= 14)
            {
                continue;
            }

            //可解析出以太网头部

            currentBytes = Arrays.copyOfRange(currentBytes, 14, currentBytes.length);

            if ((currentBytes[0] & 0xFF) == 0x45)
            {
                //IPv4 Header Length = 20

                if (currentBytes.length <= 20)
                {
                    continue;
                }

                int ipLength = StringUtil.byte2Int(Arrays.copyOfRange(currentBytes, 2, 4), 2);
                byte protocolFlag = currentBytes[9];

                currentBytes = Arrays.copyOfRange(currentBytes, 20, ipLength);

                if ((protocolFlag & 0xFF) == 0x11)
                {
                    //UDP Header Length = 8
                    if (currentBytes.length <= 8)
                    {
                        continue;
                    }

                    int udpLength = StringUtil.byte2Int(new byte[]{currentBytes[4], currentBytes[5]}, 2);

                    if (udpLength <= 8)
                    {
                        continue;
                    }
                    currentBytes = Arrays.copyOfRange(currentBytes, 8, udpLength);

                    if ((currentBytes[0] & 0xFF) == 0x30)
                    {
                        //GTP V1 Header Length = 8
                        int gtpLength = StringUtil.byte2Int(new byte[]{currentBytes[2], currentBytes[3]}, 2);
                        currentBytes = Arrays.copyOfRange(currentBytes, 8, gtpLength + 8);
                        isValid = SignallingFilterUtil.isValidS1UIPv4(currentBytes);

                    } else if ((currentBytes[0] & 0xFF) == 0x48)
                    {
                        //GTP V2

                    }
                } else if ((protocolFlag & 0xFF) == 0x06)
                {
                    //TCP
                }

            } else if ((currentBytes[2] & 0xFF) == 0x45)
            {
                //IPV4 linux client captured pcap
                isValid = SignallingFilterUtil.isValidS1UIPv4(Arrays.copyOfRange(currentBytes, 2, currentBytes.length));
            }
            if (isValid)
            {
                fos.write(bufMill);
                fos.write(bufLen);
                fos.write(rawLen);
                fos.write(dropLen);
                fos.write(myBytes);
            }
        }

        if (inStream != null)
            inStream.close();
        if (bis != null)
            bis.close();

    }


}
