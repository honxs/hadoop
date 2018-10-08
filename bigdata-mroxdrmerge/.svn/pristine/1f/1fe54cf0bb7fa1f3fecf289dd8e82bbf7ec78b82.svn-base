package com.chinamobile.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public interface FTPLoginDownload
{
	String[] ftpLogin();
	HashSet<String> listDirectoryFiles(String remotePath);
	HashMap<String,Long[]> getFileProperty(String remotePath,HashSet<String> ftpFileName);
	boolean downloadFile(String remoteFileName, String newFileName, String localDires, String remoteDownLoadPath);
    List<String> GetFolderName(String remotePath);
//    void downloadFiles(HashMap<String, Long[]> ftpFileList, int TCOUNT, String localDires,
//                              String remoteDownLoadPath);

	boolean ftpLogout();
	
}
