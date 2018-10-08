package cn.mastercom.bigdata.util;

import java.util.ArrayList;
import java.util.List;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;


@XStreamAlias("compile list")
public class CompileList
{
	@XStreamAlias("compile static mark")
	public String compile_static_mark;

	@XStreamImplicit(itemFieldName = "info list")
	public List<CompileInfo> compileList;

	public CompileList()
	{
		compile_static_mark = "";
		compileList = new ArrayList<CompileInfo>();
	}


	
	
}
