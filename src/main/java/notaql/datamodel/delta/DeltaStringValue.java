package notaql.datamodel.delta;

import notaql.datamodel.BaseAtomValue;
import notaql.datamodel.StringValue;
import notaql.datamodel.Value;

public class DeltaStringValue extends StringValue implements DeltaValue {
	// Class variables
	private static final long serialVersionUID = 345286688224502071L;
	
	
	// Object variables
	private DeltaMode mode;
	
	
	public DeltaStringValue(String value, DeltaMode mode) {
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
        
        if (o == null || !(this.getClass() == o.getClass() || o.getClass() == StringValue.class)) return false;

        BaseAtomValue<?> that = (BaseAtomValue<?>) o;

        if (this.getValue() != null ? !this.getValue().equals(that.getValue()) : that.getValue() != null) return false;

        return true;
    }
    
    
    /* (non-Javadoc)
     * @see notaql.datamodel.StringValue#deepCopy()
     */
    @Override
    public Value deepCopy() {
        return new DeltaStringValue(this.getValue(), this.getMode());
    }


	/* (non-Javadoc)
	 * @see notaql.datamodel.delta.DeltaValue#getRawValue()
	 */
	@Override
	public Value getRawValue() {
		return new StringValue(this.getValue());
	}
}
