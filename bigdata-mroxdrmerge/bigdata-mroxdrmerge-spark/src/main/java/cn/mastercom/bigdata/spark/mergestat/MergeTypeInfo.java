package cn.mastercom.bigdata.spark.mergestat;

import cn.mastercom.bigdata.util.spark.TypeInfo;

public class MergeTypeInfo extends TypeInfo
{
	private static final long serialVersionUID = 1L;
	
	protected String mergeClassName;
    protected String srcPath;
	
	public MergeTypeInfo(int type, String typeName, String typePath, String srcPath, String mergeClassName)
	{
		super(type, typeName, typePath);
		this.mergeClassName = mergeClassName;
		this.srcPath = srcPath;
	}
	
	public String getMergeClassName()
	{
		return mergeClassName;
	}
	
	public String getSrcPath()
	{
		return srcPath;
	}
	
}
