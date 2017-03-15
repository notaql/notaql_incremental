package notaql.incremental_tests;

public enum Loglevel {
	DEBUG(0),
	INFO(10),
	WARN(20),
	ERROR(30);

	private int level;

	Loglevel(int level) {
		this.level = level;
	}

	public int getLevel(){
		return this.level;
	}
}
