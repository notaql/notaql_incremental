package notaql.datamodel.delta;

import notaql.datamodel.Value;

public interface DeltaValue {
	/**
	 * @param mode the new mode
	 */
	public void setMode(DeltaMode mode);
	
	
	/**
	 * @return the mode of this value
	 */
	public DeltaMode getMode();
	
	
	/**
	 * @return true if this is a delete or an insert, false otherwise
	 */
	default public boolean isChange() {
		return this.getMode() == DeltaMode.DELETED || this.getMode() == DeltaMode.INSERTED;
	}
	
	
	/**
	 * @return the value without any delta flags
	 */
	public Value getRawValue();
}
