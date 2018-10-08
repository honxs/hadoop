package com.huawei.bigdata.mapreduce.local.lib;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * use to put the file to hdfs
 * 
 */
public class PutFileToClientServer
{

    /**
     * log process object
     */
    private static final Log LOG = LogFactory
            .getLog(PutFileToClientServer.class);

    /**
     * filesystem
     */
    private FileSystem fileSystem;

    /**
     * target path
     */
    private String destPath;

    /**
     * target file name
     */
    private String fileName;

    /**
     * source file have source path
     */
    private String sourcePath;

    /**
     * to HDFS outstream
     */
    private FSDataOutputStream hdfsOutStream;

    /**
     * HDFS BufferedOutputStream
     */
    private BufferedOutputStream bufferOutStream;

    /**
     * construct
     * 
     * @param fileSystem FileSystem : file system
     * @param destPath String : target path(remote file server)
     * @param fileName String : target file name
     * @param sourcePath String : source file(absolute path native file)
     * 
     */
    public PutFileToClientServer(FileSystem fileSystem, String destPath,
            String fileName, String sourcePath)
    {
        this.fileSystem = fileSystem;
        this.destPath = destPath;
        this.fileName = fileName;
        this.sourcePath = sourcePath;
    }

    /**
     *write to hdfs
     * 
     * @param inputStream InputStream : inputStream
     * @throws IOException , ParameterException
     */
    public void doWrite(InputStream inputStream) throws IOException,
            ParameterException
    {
        if (null == inputStream)
        {
            throw new ParameterException("some of input parameters are null.");
        }

        // Initialize
        setWriteResource();
        try
        {
            // Write to hdfs
            outputToHDFS(inputStream);
        }
        finally
        {
            closeResource();
        }
    }

    /**
     * write to the target directory
     * 
     * @param inputStream InputStream
     * @throws IOException
     */
    private void outputToHDFS(InputStream inputStream) throws IOException
    {
        final int countForOneRead = 1024; // 1024 Bytes each time
        final byte buff[] = new byte[countForOneRead];
        int count;
        while ((count = inputStream.read(buff, 0, countForOneRead)) > 0)
        {
            bufferOutStream.write(buff, 0, count);
        }

        bufferOutStream.flush();
        hdfsOutStream.hflush();
    }

    /**
     * init object
     * 
     * @throws IOException
     */
    private void setWriteResource() throws IOException
    {
        Path filepath = new Path(destPath + File.separator + fileName);
        hdfsOutStream = fileSystem.create(filepath);
        bufferOutStream = new BufferedOutputStream(hdfsOutStream);
    }

    /**
     * close resource
     */
    private void closeResource()
    {
        // Close hdfsOutStream
        if (hdfsOutStream != null)
        {
            try
            {
                hdfsOutStream.close();
            }
            catch (IOException e)
            {
                LOG.error(e);
            }
        }

        // Close bufferOutStream
        if (bufferOutStream != null)
        {
            try
            {
                bufferOutStream.close();
            }
            catch (IOException e)
            {
                LOG.error(e);
            }
        }

    }

    /**
     * put file
     * 
     * @return boolean : result
     */
    public boolean put()
    {

        if (fileSystem == null)
        {

            LOG.error("Failed to write to client server because of the fileSystem is null.");
            return false;
        }

        // Create target file directory
        Path destFilePath = new Path(destPath);
        if (!createPath(destFilePath))
        {
            LOG.error("failed to write because the destPath " + destPath
                    + "cannot be created ");
            return false;
        }

        // Inputstream
        FileInputStream input = null;
        try
        {
            input = new FileInputStream(sourcePath);
            doWrite(input);

        }
        catch (FileNotFoundException e)
        {
            LOG.error("failed to put the file to client server." + e);
            return false;
        }
        catch (ParameterException e1)
        {
            LOG.error("failed to put the file to client server." + e1);
            return false;
        }
        catch (IOException e2)
        {
            LOG.error("failed to put the file to client server." + e2);
            return false;
        }
        finally
        {
            close(input);
        }

        return true;
    }

    /**
     * create a file path
     * 
     * @param filePath Path : file path
     * @return boolean : result
     */
    private boolean createPath(final Path filePath)
    {

        if (filePath == null)
        {
            LOG.error("Failed to create the file path because of it is null.");
            return false;
        }

        try
        {
            if (!fileSystem.exists(filePath))
            {

                // Create this directory
                fileSystem.mkdirs(filePath);
            }
            return true;
        }
        catch (IOException e)
        {
            LOG.error("Failed to create the file path. " + e);
            return false;
        }
    }

    /**
     * close stream
     * 
     * @param stream Closeable : stream object
     * 
     */
    private void close(Closeable stream)
    {

        if (stream == null)
        {
            return;
        }

        try
        {
            stream.close();
        }
        catch (IOException e)
        {
            LOG.error("Failed to close the stream. " + e);
        }
    }

    /**
     * put local file to remote file system
     * 
     * @param fileSystem FileSystem : file system
     * @param fileConfig String : conf files need to put hdfs 
     * @param localPath String : local file directory
     * @param inputPath String : remote target path
     * @return boolean : result
     */
    public static boolean putFiles(FileSystem fileSystem,
            final String fileConfig, final String localPath,
            final String inputPath)
    {

        // Put to hdfs files
        String[] filenames = fileConfig.split(",");

        if (filenames == null || filenames.length <= 0)
        {
            LOG.error("There is no files could to put to the client server.");
            return false;
        }

        // Put hdfs class
        PutFileToClientServer putFile = null;

        boolean result = false;

        for (int i = 0; i < filenames.length; i++)
        {
            if (filenames[i] == null || "".equals(filenames[i]))
            {
                continue;
            }

            // Excute put hdfs
            putFile = new PutFileToClientServer(fileSystem, inputPath,
                    filenames[i], localPath + File.separator + filenames[i]);
            result = putFile.put();
            if (result == false)
            {
                LOG.error("Failed to put the local file to client server. ");
                return false;
            }
        }

        return true;
    }

}

/**
 * parameter exception class
 * 
 */
class ParameterException extends Exception
{

    private static final long serialVersionUID = -1423752061047262889L;

    public ParameterException(String msg)
    {
        super(msg);
    }

}