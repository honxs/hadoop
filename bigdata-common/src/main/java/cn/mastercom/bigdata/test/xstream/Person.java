package cn.mastercom.bigdata.test.xstream;

import java.util.List;

public class Person
{
	private String name;
	private String age;
	private Profile profile;
	private List<Address> addlist;

	public Person(String name, String age, Profile profile, List<Address> addlist)
	{
		this.name = name;
		this.age = age;
		this.profile = profile;
		this.addlist = addlist;
	}

	public String toString()
	{
		return "Person{" + "name='" + name + '\'' + ", age='" + age + '\'' + ", profile=" + profile + ", addlist="
				+ addlist + '}';
	}
}
