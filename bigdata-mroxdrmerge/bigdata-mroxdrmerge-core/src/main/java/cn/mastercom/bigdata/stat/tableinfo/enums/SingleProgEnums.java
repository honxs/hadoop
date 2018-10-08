package cn.mastercom.bigdata.stat.tableinfo.enums;

import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;

public enum SingleProgEnums implements IOutPutPathEnum
{
	EciFilter_MME("ecifiltermme", ""),
	EciFilter_HTTP("ecifilterhttp",""),
	EciFilter_MRO("ecifiltermro",""),
	EciFilter_XDRLOCATION("ecifilterxdrlocation",""),
	FreqLoc("FreqLoc","")
	;
	private int index;
	private String fileName;
	private String dirName;

	private SingleProgEnums(String fileName, String dirName)
	{
		this.fileName = fileName;
		this.dirName = dirName;
		this.index=SingleProgEnums.class.getName().hashCode()+ordinal();
	}

	@Override
	public String getPath(String hPath, String outData)
	{
		return hPath + "/" + dirName + "_" + outData;
	}

	/**
	 * 返回输入路径
	 * @param path
	 * @return
	 */
	public String getPath(String path)
	{
			return path;
	}

	@Override
	public String getFileName()
	{
		return fileName;
	}

	@Override
	public String getDirName()
	{
		return dirName;
	}

	@Override
	public int getIndex()
	{
		return index;
	}

	@Override
	public String getHourPath(String hPath, String outData, String hour) {
		// TODO Auto-generated method stub
		return null;
	}

}
