package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MyNumber implements WritableComparable<MyNumber> {
	
	private int first;
	private int second;

	@Override
	public void readFields(DataInput arg0) throws IOException {
		first = arg0.readInt();
		second = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(first);
		arg0.writeInt(second);
	}

	@Override
	public int compareTo(MyNumber o) {
		if(this.first != o.first){
			return this.getFirst() > o.getFirst() ? 1 : -1;
		}else if(this.second != o.second){
				return this.getSecond() > o.getSecond() ? 1 : -1;
		}else {
				return 0;
		}
	}
	
	@Override
	public int hashCode() {
		return first * 157 + second;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(null == obj){
			return false;
		}
		if(obj == this){
			return true;
		}
		if(obj instanceof MyNumber){
			return ((MyNumber) obj).getFirst() == first && ((MyNumber) obj).getSecond() == second;
		}else{
			return false;
		}
	}
	
	public void setFirst(int first) {
		this.first = first;
	}
	
	public int getFirst() {
		return first;
	}
	
	public void setSecond(int second) {
		this.second = second;
	}
	
	public int getSecond() {
		return second;
	}

}
