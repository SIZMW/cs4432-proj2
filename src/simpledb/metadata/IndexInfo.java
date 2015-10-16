package simpledb.metadata;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.BLOCK_SIZE;

import simpledb.index.Index;
import simpledb.index.IndexType;
import simpledb.index.btree.BTreeIndex;
import simpledb.index.hash.ExtensibleHashIndex;
import simpledb.index.hash.HashIndex;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * The information about an index. This information is used by the query planner
 * in order to estimate the costs of using the index, and to obtain the schema
 * of the index records. Its methods are essentially the same as those of Plan.
 *
 * @author Edward Sciore
 */
public class IndexInfo {
    private String idxname, fldname;
    private Transaction tx;
    private TableInfo ti;
    private StatInfo si;

    /**
     * CS 4432 Project 2
     *
     * We added the index type to store the index type.
     */
    protected IndexType type;

    /**
     * CS 4432 Project 2
     *
     * We added the index type to pass in the index type.
     *
     * Creates an IndexInfo object for the specified index.
     *
     * @param idxname
     *            the name of the index
     * @param tblname
     *            the name of the table
     * @param fldname
     *            the name of the indexed field
     * @param tx
     *            the calling transaction
     */
    public IndexInfo(String idxname, String tblname, String fldname, Transaction tx, IndexType type) {
        this.idxname = idxname;
        this.fldname = fldname;
        this.tx = tx;
        this.type = type;
        ti = SimpleDB.mdMgr().getTableInfo(tblname, tx);
        si = SimpleDB.mdMgr().getStatInfo(tblname, ti, tx);
    }

    /**
     * CS 4432 Project 2
     *
     * We added the check of index type to create or open the index of the
     * specified type. If incorrect types are passed in, a default HashIndex is
     * used.
     *
     * Opens the index described by this object.
     *
     * @return the Index object associated with this information
     */
    public Index open() {
        Schema sch = schema();

        // Select index type based on type parameters
        if (type.equals(IndexType.sh)) {
            return new HashIndex(idxname, sch, tx);
        } else if (type.equals(IndexType.bt)) {
            return new BTreeIndex(idxname, sch, tx);
        } else if (type.equals(IndexType.eh)) {
            return new ExtensibleHashIndex(idxname, sch, tx);
        } else {
            // Return hash index if arguments are invalid
            return new HashIndex(idxname, sch, tx);
        }
    }

    /**
     * Estimates the number of block accesses required to find all index records
     * having a particular search key. The method uses the table's metadata to
     * estimate the size of the index file and the number of index records per
     * block. It then passes this information to the traversalCost method of the
     * appropriate index type, which provides the estimate.
     *
     * @return the number of block accesses required to traverse the index
     */
    public int blocksAccessed() {
        // TODO Change estimation to be relative to the index type
        TableInfo idxti = new TableInfo("", schema());
        int rpb = BLOCK_SIZE / idxti.recordLength();
        int numblocks = si.recordsOutput() / rpb;
        // Call HashIndex.searchCost for hash indexing
        return HashIndex.searchCost(numblocks, rpb);
    }

    /**
     * Returns the estimated number of records having a search key. This value
     * is the same as doing a select query; that is, it is the number of records
     * in the table divided by the number of distinct values of the indexed
     * field.
     *
     * @return the estimated number of records having a search key
     */
    public int recordsOutput() {
        return si.recordsOutput() / si.distinctValues(fldname);
    }

    /**
     * Returns the distinct values for a specified field in the underlying
     * table, or 1 for the indexed field.
     *
     * @param fname
     *            the specified field
     */
    public int distinctValues(String fname) {
        if (fldname.equals(fname))
            return 1;
        else
            return Math.min(si.distinctValues(fldname), recordsOutput());
    }

    /**
     * Returns the schema of the index records. The schema consists of the
     * dataRID (which is represented as two integers, the block number and the
     * record ID) and the dataval (which is the indexed field). Schema
     * information about the indexed field is obtained via the table's metadata.
     *
     * @return the schema of the index records
     */
    private Schema schema() {
        Schema sch = new Schema();
        sch.addIntField("block");
        sch.addIntField("id");
        if (ti.schema().type(fldname) == INTEGER)
            sch.addIntField("dataval");
        else {
            int fldlen = ti.schema().length(fldname);
            sch.addStringField("dataval", fldlen);
        }
        return sch;
    }
}
