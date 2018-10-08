package cn.mastercom.bigdata.test.xstream;

public class Address
{
	private String add;
	private String zipcode;

	public Address(String add, String zipcode)
	{
		this.add = add;
		this.zipcode = zipcode;
	}

	public String toString()
	{
		return "Address{" + "add='" + add + '\'' + ", zipcode='" + zipcode + '\'' + '}';
	}
}
