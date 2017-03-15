package notaql.datamodel.delta;

import org.bson.types.ObjectId;

import notaql.datamodel.BaseAtomValue;
import notaql.datamodel.Value;
import notaql.engines.mongodb.datamodel.ObjectIdValue;

public class DeltaObjectIdValue extends ObjectIdValue implements DeltaValue {
	// Class variables
	private static final long serialVersionUID = -4347749810140880957L;
	
	
	// Object variables
	private DeltaMode mode;
	
	
	public DeltaObjectIdValue(ObjectId value, DeltaMode mode) {
		super(value);
		this.mode = mode;
	}
	
	
	/* (non-Javadoc)
	 * @see notaql.datamodel.BaseAtomValue#toString()
	 */
	public String toString() {
		return super.toString() + " (" +  this.mode +  ")";
	}


	/* (non-Javadoc)
	 * @see notaql.datamodel.timestamp.TimestampValue#getMode()
	 */
	@Override
	public DeltaMode getMode() {
		return this.mode;
	}
	
	
	/* (non-Javadoc)
	 * @see notaql.datamodel.timestamp.TimestampValue#setMode(notaql.datamodel.timestamp.TimestampMode)
	 */
	@Override
	public void setMode(DeltaMode mode) {
		this.mode = mode;
	}
	

    /* (non-Javadoc)
     * @see notaql.datamodel.BaseAtomValue#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
        	return true;
        
        if (o == null || !(this.getClass() == o.getClass() || o.getClass() == ObjectIdValue.class)) return false;

        BaseAtomValue<?> that = (BaseAtomValue<?>) o;

        if (this.getValue() != null ? !this.getValue().equals(that.getValue()) : that.getValue() != null) return false;

        return true;
    }
    
    
    /* (non-Javadoc)
     * @see notaql.engines.mongodb.datamodel.ObjectIdValue#deepCopy()
     */
    @Override
    public Value deepCopy() {
        return new DeltaObjectIdValue(this.getValue(), this.getMode());
    }


	/* (non-Javadoc)
	 * @see notaql.datamodel.delta.DeltaValue#getRawValue()
	 */
	@Override
	public Value getRawValue() {
		return new ObjectIdValue(this.getValue());
	}
}
