package flink.examples.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.hadoop.io.Writable;

public class StringArrayWritable implements Writable, Comparable<StringArrayWritable> {
    
    private String[] array = new String[0];

    public StringArrayWritable() {
        super();
    }

    public StringArrayWritable(String[] array) {
        this.array = array;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.array.length);

        for (String str : this.array) {
            byte[] b = str.getBytes(ConfigConstants.DEFAULT_CHARSET);
            out.writeInt(b.length);
            out.write(b);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.array = new String[in.readInt()];

        for (int i = 0; i < this.array.length; i++) {
            byte[] b = new byte[in.readInt()];
            in.readFully(b);
            this.array[i] = new String(b, ConfigConstants.DEFAULT_CHARSET);
        }
    }

    @Override
	public int compareTo(StringArrayWritable o) {
		if (this.array.length != o.array.length) {
			return this.array.length - o.array.length;
		}

		for (int i = 0; i < this.array.length; i++) {
			int comp = this.array[i].compareTo(o.array[i]);
			if (comp != 0) {
				return comp;
			}
		}
		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof StringArrayWritable)) {
			return false;
		}
		return this.compareTo((StringArrayWritable) obj) == 0;
	}
}
