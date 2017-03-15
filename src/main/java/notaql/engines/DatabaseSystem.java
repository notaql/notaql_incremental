package notaql.engines;

/**
 * This engine is backed by a database system.
 */
public interface DatabaseSystem {
    /**
     * @return a reference to the current used database
     */
    public DatabaseReference getDatabaseReference();
}
