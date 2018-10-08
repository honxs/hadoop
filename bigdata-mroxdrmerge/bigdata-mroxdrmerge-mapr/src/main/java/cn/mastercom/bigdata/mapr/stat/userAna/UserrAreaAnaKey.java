package cn.mastercom.bigdata.mapr.stat.userAna;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserrAreaAnaKey implements WritableComparable<UserrAreaAnaKey>{

	private String imsi;
	
	private long time;
	
	public UserrAreaAnaKey() {}

	public UserrAreaAnaKey(String imsi, long time) {
		this.imsi = imsi;
		this.time = time;
	}

	public String getImsi() {
		return imsi;
	}

	public void setImsi(String imsi) {
		this.imsi = imsi;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		imsi = in.readUTF();
		time = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(imsi);
		out.writeLong(time);
		
	}

	@Override
	public int compareTo(UserrAreaAnaKey obj) {
		int result = imsi.compareTo(obj.getImsi());
		/*if(result == 0){
			result = Long.compare(time, obj.getTime());
		}*/
		return result;
	}
}


