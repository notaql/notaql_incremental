package notaql.datamodel.delta;

import notaql.datamodel.BaseAtomValue;
import notaql.datamodel.NumberValue;
import notaql.datamodel.Value;

public class DeltaNumberValue extends NumberValue implements DeltaValue {
	// Class variables
	private static final long serialVersionUID = -4347749810140880957L;
	
	
	// Object variables
	private DeltaMode mode;
	
	
	public DeltaNumberValue(Number value, DeltaMode mode) {
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

        if (o == null || !(this.getClass() == o.getClass() || o.getClass() == NumberValue.class)) return false;

        BaseAtomValue<?> that = (BaseAtomValue<?>) o;

        if (this.getValue() != null ? !this.getValue().equals(that.getValue()) : that.getValue() != null) return false;

        return true;
    }

    
    /* (non-Javadoc)
     * @see notaql.datamodel.NumberValue#deepCopy()
     */
    @Override
    public Value deepCopy() {
        return new DeltaNumberValue(this.getValue(), this.getMode());
    }


	/* (non-Javadoc)
	 * @see notaql.datamodel.delta.DeltaValue#getRawValue()
	 */
	@Override
	public Value getRawValue() {
		return new NumberValue(this.getValue());
	}
}
