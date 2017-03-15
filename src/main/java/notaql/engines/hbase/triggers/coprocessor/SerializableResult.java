package notaql.engines.hbase.triggers.coprocessor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

/**
 * Result (HBase) does not implement serializable. This class is a work-around.
 */
public class SerializableResult implements Serializable {
	// Configuration
	private static final long serialVersionUID = 5360224695258531166L;
	
	
	// Object variables
	private final ArrayList<SerializableCell> serializableCells;
	
	
	public SerializableResult(Result result) {
		this(result.listCells());
	}
	
	
	public SerializableResult(Collection<Cell> cells) {
    	if (cells == null)
    		this.serializableCells = new ArrayList<SerializableCell>(0);
    	else {
    		this.serializableCells = new ArrayList<SerializableCell>(cells.size());
    		
    		for (Cell cell : cells)
    			this.serializableCells.add(new SerializableCell(cell));
    	}
	}
	
	
	public SerializableResult(ArrayList<SerializableCell> serializableCells) {
		this.serializableCells = serializableCells;
	}
	
	
	/**
	 * Generates a Result from the serialized cells.
	 * 
	 * @return
	 */
	public Result toResult() {
		// Create normal cells
		List<Cell> cells = this.serializableCells
			.stream()
			.map(SerializableCell::toCell)
			.collect(Collectors.toList());
		
		
		// Create the Result
		return Result.create(cells);
	}
	
	
	/**
	 * Serializes the object.
	 * 
	 * @return
	 * @throws IOException
	 */
	public String toSerializedString() throws IOException {
		return serializableToString(this.serializableCells);
	}
	
	
	/**
	 * Deserializes the object.
	 * 
	 * @param string
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static SerializableResult fromSerializedString(String string) throws IOException {
		Serializable serializable;
		try {
			serializable = stringToSerializable(string);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Can't deserialize String: " + e.toString());
		}
		
		if (serializable instanceof ArrayList)
			return new SerializableResult((ArrayList<SerializableCell>) serializable);
		else
			throw new IllegalArgumentException("String is no ArrayList");
	}

	
	/**
	 * Converts the serializable object into a string.
	 * 
	 * @param object
	 * @return
	 * @throws IOException
	 */
	private static String serializableToString(Serializable object) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
		objectOutputStream.writeObject(object);
		objectOutputStream.flush();
		
		String returnString = Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());

		objectOutputStream.close();
		byteArrayOutputStream.close();
		
		return returnString;
	}
	
	
	/**
	 * Converts the serialized string into an object.
	 * 
	 * @param string
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	private static Serializable stringToSerializable(String string) throws IOException, ClassNotFoundException {
		byte[] data = Base64.getDecoder().decode(string);
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
	    ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
	    
	    Serializable serializable = (Serializable) objectInputStream.readObject();
	    
	    objectInputStream.close();
	    byteArrayInputStream.close();
	    
	    return serializable;
	}
}
