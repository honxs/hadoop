package cn.mastercom.bigdata.standalone.stat.mrloc;

import cn.mastercom.bigdata.standalone.main.StartUp;

public class MrLocMain
{
	public static void main(String[] args)
	{
		StartUp startUp = new StartUp(new MrLocWork());
		startUp.start();
	}
}
