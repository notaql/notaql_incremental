package notaql.engines.hbase.triggers.coprocessor;

import java.io.Serializable;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

public class SerializableCell implements Serializable {
	// Configuration
	private static final long serialVersionUID = 2846081836417866469L;
	
	
	// Object variables
	public final byte[] row;
	public final byte [] family;
	public final byte [] qualifier;
	public final long timestamp;
	public final byte type;
	public final byte [] value;
	
	
	public SerializableCell(Cell cell) {
		this.row = CellUtil.cloneRow(cell);
		this.family = CellUtil.cloneFamily(cell);
		this.qualifier = CellUtil.cloneQualifier(cell);
		this.timestamp = cell.getTimestamp();
		this.type = cell.getTypeByte();
		this.value= CellUtil.cloneValue(cell);
	}
	
	
	public Cell toCell() {
		return CellUtil.createCell(row, family, qualifier, timestamp, type, value);
	}
}
