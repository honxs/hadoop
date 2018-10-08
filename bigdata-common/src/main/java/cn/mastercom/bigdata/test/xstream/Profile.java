package cn.mastercom.bigdata.test.xstream;

public class Profile
{
	private String job;
	private String tel;
	private String remark;

	public Profile(String job, String tel, String remark)
	{
		this.job = job;
		this.tel = tel;
		this.remark = remark;
	}

	public String toString()
	{
		return "Profile{" + "job='" + job + '\'' + ", tel='" + tel + '\'' + ", remark='" + remark + '\'' + '}';
	}
}
