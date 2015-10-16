package simpledb.metadata;

import static simpledb.metadata.TableMgr.MAX_NAME;

import java.util.HashMap;
import java.util.Map;

import simpledb.index.IndexType;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/**
 * The index manager. The index manager has similar functionality to the table
 * manager.
 *
 * @author Edward Sciore
 */
public class IndexMgr {
    private TableInfo ti;

    /**
     * CS 4432 Project 2
     *
     * We added another field to contain the index type.
     *
     * Creates the index manager. This constructor is called during system
     * startup. If the database is new, then the <i>idxcat</i> table is created.
     *
     * @param isnew
     *            indicates whether this is a new database
     * @param tx
     *            the system startup transaction
     */
    public IndexMgr(boolean isnew, TableMgr tblmgr, Transaction tx) {
        if (isnew) {
            Schema sch = new Schema();
            sch.addStringField("indexname", MAX_NAME);
            sch.addStringField("tablename", MAX_NAME);
            sch.addStringField("fieldname", MAX_NAME);

            // Added this field for index type.
            sch.addStringField("indextype", MAX_NAME);
            tblmgr.createTable("idxcat", sch, tx);
        }
        ti = tblmgr.getTableInfo("idxcat", tx);
    }

    /**
     * CS 4432 Project 2
     *
     * We added another parameter to pass in the index type.
     *
     * Creates an index of the specified type for the specified field. A unique
     * ID is assigned to this index, and its information is stored in the idxcat
     * table.
     *
     * @param idxname
     *            the name of the index
     * @param tblname
     *            the name of the indexed table
     * @param fldname
     *            the name of the indexed field
     * @param tx
     *            the calling transaction
     * @param type
     *            The type of index to be created.
     */
    public void createIndex(String idxname, String tblname, String fldname, Transaction tx, IndexType type) {
        RecordFile rf = new RecordFile(ti, tx);
        rf.insert();
        rf.setString("indexname", idxname);
        rf.setString("tablename", tblname);
        rf.setString("fieldname", fldname);

        // Sets the index type field to the input index type
        rf.setString("indextype", type.toString());
        rf.close();
    }

    /**
     * CS 4432 Project 2
     *
     * We added the "indextype" field to the record file for index type
     * information.
     *
     * Returns a map containing the index info for all indexes on the specified
     * table.
     *
     * @param tblname
     *            the name of the table
     * @param tx
     *            the calling transaction
     * @return a map of IndexInfo objects, keyed by their field names
     */
    public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
        Map<String, IndexInfo> result = new HashMap<String, IndexInfo>();
        RecordFile rf = new RecordFile(ti, tx);
        while (rf.next())
            if (rf.getString("tablename").equals(tblname)) {
                String idxname = rf.getString("indexname");
                String fldname = rf.getString("fieldname");

                // Gets the index type field information
                String indextype = rf.getString("indextype");
                IndexInfo ii = new IndexInfo(idxname, tblname, fldname, tx, IndexType.valueOf(indextype));
                result.put(fldname, ii);
            }
        rf.close();
        return result;
    }
}
