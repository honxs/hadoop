package cn.mastercom.bigdata.standalone.local;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.project.enums.IStandAloneEnum;
import cn.mastercom.bigdata.util.IDataOutputer;

public class FileTypeResult implements IDataOutputer
{
	public String outpath_mode = "dd";
	private Map<Integer, FileStore> markFileStoreMap;

	public FileTypeResult(String outpath_root, String outpath_name, String outpath_date, IStandAloneEnum[] dataTypes)
	{
		markFileStoreMap = new HashMap<Integer, FileStore>();

		for (IStandAloneEnum iResultEnum : dataTypes)
		{
			int mark = iResultEnum.getMark();
			String tableName = iResultEnum.getName();
			String file = IOHelper.pathCombine(outpath_root, tableName + "_" + outpath_mode + "_" + outpath_date,
					outpath_name + ".bcp");
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

	}

}
