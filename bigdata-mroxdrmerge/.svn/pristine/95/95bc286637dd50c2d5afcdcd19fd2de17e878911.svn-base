package cn.mastercom.bigdata.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;

public class FileTypeResult implements IDataOutputer
{
	public String outpath_mode = "dd";
	private Map<Integer, FileStore> markFileStoreMap;

	public FileTypeResult(String outpath_root, String outpath_date, IOutPutPathEnum[] dataTypes)
	{
		markFileStoreMap = new HashMap<Integer, FileStore>();

		for (IOutPutPathEnum iResultEnum : dataTypes)
		{
			int mark = iResultEnum.getIndex();
			String file = iResultEnum.getPath(outpath_root, outpath_date) + "/" + iResultEnum.getFileName();
			FileStore store = new FileStore(file);
			markFileStoreMap.put(mark, store);
		}
	}

	private FileStore getStore(int mark)
	{
		if (markFileStoreMap.containsKey(mark))
		{
			return markFileStoreMap.get(mark);
		}
		else
		{
			return null;
		}
	}

	@Override
	public int pushData(int dataType, String data)
	{
		FileStore store = getStore(dataType);
		if (store == null) return -1;
		store.AddLine(data);
		if (markFileStoreMap.size() >= 100000){
			Flush();
		}
		return 0;
	}

	public void Flush()
	{
		for (FileStore store : markFileStoreMap.values())
		{
			store.Flush();
		}
	}

	@Override
	public int pushData(int dataType, List<String> valueList)
	{
		FileStore store = getStore(dataType);
		if (store == null) return -1;
		store.AddLine(valueList);
		return 0;
	}

	@Override
	public void clear()
	{
		Flush();
	}

}
