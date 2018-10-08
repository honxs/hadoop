package cn.mastercom.bigdata.mapr.stat.userAna;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class PotentialUserAnaKey implements WritableComparable<PotentialUserAnaKey>{

	public PotentialUserAnaKey(){}
	
	public PotentialUserAnaKey(int dataType){
		this.dataType = dataType;
	}
	
	private int dataType;
	
	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		dataType = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(dataType);
	}

	@Override
	public int compareTo(PotentialUserAnaKey o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
