package cn.mastercom.bigdata.mapr.stat.userAna;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class UserAnaSectionKey implements WritableComparable<UserAnaSectionKey>{

	public UserAnaSectionKey(){}
	
	public UserAnaSectionKey(int hourTime, boolean oneToTwo, int sectionId, int dataType){
		this.hourTime = hourTime;
		this.oneToTwo = oneToTwo;
		this.dataType = dataType;
		this.sectionId = sectionId;
	}
	
	private boolean oneToTwo;
	
	private int dataType;
	
	private int sectionId;
	
	private int hourTime;
	
	public int getHourTime() {
		return hourTime;
	}

	public void setHourTime(int hourTime) {
		this.hourTime = hourTime;
	}

	public boolean isOneToTwo() {
		return oneToTwo;
	}

	public void setOneToTwo(boolean oneToTwo) {
		this.oneToTwo = oneToTwo;
	}

	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

	public int getSectionId() {
		return sectionId;
	}

	public void setSectionId(int sectionId) {
		this.sectionId = sectionId;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		oneToTwo = in.readBoolean();
		dataType = in.readInt();
		sectionId = in.readInt();
		hourTime = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(oneToTwo);
		out.writeInt(dataType);
		out.writeInt(sectionId);
		out.writeInt(hourTime);
	}

	@Override
	public int compareTo(UserAnaSectionKey o) {
		int result = Integer.compare(dataType, o.getDataType());
		if(result == 0){
			result = Integer.compare(sectionId, o.getSectionId());
		}
		if(result == 0){
			result = Boolean.compare(oneToTwo, o.isOneToTwo());
		}
		if(result == 0){
			result = Integer.compare(hourTime, o.getHourTime());
		}
		return result;
	}

	@Override
	public String toString() {
		return ""+dataType+"\t"+sectionId+"\t"+oneToTwo+"\t"+hourTime;
	}

	@Override
	public int hashCode() {
		
		return Objects.hash(dataType, sectionId, oneToTwo, hourTime);
	}
	
	

}
