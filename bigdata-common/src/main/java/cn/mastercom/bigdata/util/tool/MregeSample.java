package cn.mastercom.bigdata.util.tool;

import java.io.File;
import java.util.HashMap;
import java.util.List;

import cn.mastercom.bigdata.util.LocalFile;

public class MregeSample {

	public static void main(String[] args) {
		if(args.length !=2)
		{
			System.out.println("usage inputpath outputpath");
			return;
		}
		SxSampleMerge(args[0], args[1]);
	}

	public static void  SxSampleMerge(String inputPath, String outputPath)
	{
		List<String> files;

		HashMap<String, Integer> mapValue = new HashMap<String, Integer>();

		try {
			files = LocalFile.getAllDirs(new File(inputPath), "sample", 10, 0,1000);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("e2:" + e.getMessage());
			return;
		}

		if (files.size() == 0)
			return;

		for (String fileName : files) {
			File file = new File(fileName);
			if (!fileName.toLowerCase().contains("dd"))
				continue;

			String name = file.getName();
			int pos = name.indexOf("dd");
			String type = name.substring(0,pos-1);
			LocalFile.MergeFile(fileName, outputPath + "/" + type, name + ".txt", "", false);
		}
	}
	
}
