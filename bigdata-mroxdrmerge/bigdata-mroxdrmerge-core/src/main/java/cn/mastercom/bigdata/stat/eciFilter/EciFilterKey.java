package cn.mastercom.bigdata.stat.eciFilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class EciFilterKey implements WritableComparable<EciFilterKey> {
	public long eci;
	public int dataType;

	public EciFilterKey() {

	}

	public EciFilterKey(long eci, int dataType) {
		super();
		this.eci = eci;
		this.dataType = dataType;
	}

	public long getEci() {
		return eci;
	}

	public void setEci(long eci) {
		this.eci = eci;
	}

	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.eci = in.readLong();
		this.dataType = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.eci);
		out.writeInt(this.dataType);
	}

	@Override
	public int compareTo(EciFilterKey o) {
		if (this.eci > o.eci) {
			return 1;
		} else if (this.eci < o.eci) {
			return -1;
		} else {
			if (this.dataType > o.dataType) {
				return 1;
			} else if (this.dataType < o.dataType) {
				return -1;
			} else {
				return 0;
			}
		}
	}

	public String toString() {
		return eci + "," + dataType;
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (this == obj) {
			return true;
		} else if (obj instanceof EciFilterKey) {
			EciFilterKey s = (EciFilterKey) obj;
			return eci == s.eci && dataType == s.dataType;
		} else {
			return false;
		}
	}

}
