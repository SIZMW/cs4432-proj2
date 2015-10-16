package simpledb.index.hash;

import java.util.ArrayList;
import java.util.List;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/**
 * CS 4432 Project 2
 *
 * This class is the extensible hash index implementation.
 *
 * @author Aditya Nivarthi
 */
public class ExtensibleHashIndex implements Index {

    // Current values
    protected int currentBucketCount = 0;
    protected int currentBucketNumber = 0;
    protected int currentBitCount = 1;

    // Extensible hash index information
    protected String idxname;
    protected Schema sch;
    protected Transaction tx;
    protected Constant searchkey = null;
    protected TableScan ts = null;

    // Index file name
    protected final String INDEX_FILENAME = "ehindexfile";

    // Index field names
    protected final String BUCKET_NUM = "bucketnum";
    protected final String BUCKET_BITS = "buckebitse";
    protected final String BUCKET_TUPLES = "buckettuples";
    protected final String BUCKET_FILE_NAME = "indexfilename";

    // Maximum number of tuples in an index bucket
    protected final int NUM_BUCKET_TUPLES = 20000;

    // Index information
    protected Schema indexBucketSchema;
    protected TableInfo indexBucketTableInfo;
    protected TableScan indexBucketTableScan;

    // Hash value for modulo
    protected static final int HASH_MOD_VAL = 1610612741;

    /**
     * Opens an extensible hash index for the specified index.
     *
     * @param idxname
     *            the name of the index
     * @param sch
     *            the schema of the index records
     * @param tx
     *            the calling transaction
     */
    public ExtensibleHashIndex(String idxname, Schema sch, Transaction tx) {
        this.idxname = idxname;
        this.sch = sch;
        this.tx = tx;

        // TODO Global extensible hash index values are not correct when
        // rereading from disk

        // Set up the bucket table schema
        indexBucketSchema = new Schema();
        indexBucketSchema.addIntField(BUCKET_NUM);
        indexBucketSchema.addIntField(BUCKET_BITS);
        indexBucketSchema.addIntField(BUCKET_TUPLES);
        indexBucketSchema.addStringField(BUCKET_FILE_NAME, 10);

        indexBucketTableInfo = new TableInfo(INDEX_FILENAME, indexBucketSchema);
        indexBucketTableScan = new TableScan(indexBucketTableInfo, tx);

        indexBucketTableScan.beforeFirst();
    }

    /**
     * This method closes the previous table scans if any and resets the current
     * bucket number being evaluated to the new search key.
     *
     * (non-Javadoc)
     *
     * @see simpledb.index.Index#beforeFirst(simpledb.query.Constant)
     */
    @Override
    public void beforeFirst(Constant searchkey) {
        close();
        this.searchkey = searchkey;
        currentBucketNumber = searchkey.hashCode() % HASH_MOD_VAL;
    }

    @Override
    public boolean next() {
        while (ts.next()) {
            if (ts.getVal("dataval").equals(searchkey)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Retrieves the dataRID from the current record in the table scan for the
     * bucket.
     *
     * @see simpledb.index.Index#getDataRid()
     */
    @Override
    public RID getDataRid() {
        int blknum = ts.getInt("block");
        int id = ts.getInt("id");
        return new RID(blknum, id);
    }

    /**
     * Returns a mask for the number of bits to leave unmasked starting from 2^0
     * and moving up.
     *
     * @param bits
     *            The number of bits to leave unmasked from the rightmost bit.
     * @return an integer
     */
    protected int getMask(int bits) {
        int sum = 0;
        for (int i = bits - 1; i >= 0; i--) {
            sum = sum | (int) Math.pow(2, i);
        }

        return sum;
    }

    /**
     * CS 4432 Project 2
     *
     * Inserts a new record into the table scan for the bucket.
     *
     * @see simpledb.index.Index#insert(simpledb.query.Constant,
     *      simpledb.record.RID)
     */
    @Override
    public void insert(Constant val, RID rid) {
        beforeFirst(val);
        indexBucketTableScan.beforeFirst();

        // Check if there is an existing bucket where the record can be placed
        while (indexBucketTableScan.next()) {
            // Get both the current hash value masked and the bucket value
            // masked at the bucket's bit number
            int bucketNumberMask = indexBucketTableScan.getInt(BUCKET_NUM)
                    & getMask(indexBucketTableScan.getInt(BUCKET_BITS));

            int currentBucketNumberMask = currentBucketNumber & getMask(indexBucketTableScan.getInt(BUCKET_BITS));

            // Check if both masked values are equal
            if (bucketNumberMask == currentBucketNumberMask) {
                // If the bucket is full or is about to be full
                if (indexBucketTableScan.getInt(BUCKET_TUPLES) >= NUM_BUCKET_TUPLES) {
                    reorganizeBucketRecords(val, rid);
                    return;
                } else {
                    insertExistingIndexBucket(val, rid);
                    printExtensibleHashIndexInformation();
                    return;
                }
            }
        }

        // Insert a new bucket for the new value at the global number of bits
        insertNewIndexBucket(val, rid, currentBucketNumber & getMask(currentBitCount), currentBitCount);
        printExtensibleHashIndexInformation();
    }

    /**
     * CS 4432 Project 2
     *
     * Reorganizes the bucket values in order to expand the directory size and
     * resort the values at the new bit values.
     *
     * @param val
     *            The value to be inserted.
     * @param rid
     *            The RID of the value to be inserted.
     */
    protected void reorganizeBucketRecords(Constant val, RID rid) {
        int prevBucketBits = indexBucketTableScan.getInt(BUCKET_BITS);
        indexBucketTableScan.setInt(BUCKET_BITS, indexBucketTableScan.getInt(BUCKET_BITS) + 1);

        // Check if global index needs to be incremented
        // TODO Check if this should be >= or just >
        if (indexBucketTableScan.getInt(BUCKET_BITS) >= currentBitCount) {
            currentBitCount++;
        }

        // Open the file for the current bucket that is full
        String fileName = indexBucketTableScan.getString(BUCKET_FILE_NAME);
        TableScan tblsn = getTableByFileName(fileName);
        tblsn.beforeFirst();

        List<Constant> resortList = new ArrayList<Constant>();

        // Check each record in the bucket file
        while (tblsn.next()) {
            Constant key = tblsn.getVal("dataval");
            int bucket = key.hashCode() % HASH_MOD_VAL;

            // Get the masked values for the record and the bucket's new bit
            // value
            int checkBucketNumMask = indexBucketTableScan.getInt(BUCKET_NUM)
                    & getMask(indexBucketTableScan.getInt(BUCKET_BITS));

            int bucketNumMask = bucket & getMask(indexBucketTableScan.getInt(BUCKET_BITS));

            // If the record does not hash here after the bit value increment,
            // mark it for removal and resorting
            if (bucketNumMask != checkBucketNumMask) {
                resortList.add(key);
                indexBucketTableScan.setInt(BUCKET_TUPLES, indexBucketTableScan.getInt(BUCKET_TUPLES) - 1);
                tblsn.delete();
            }
        }

        int b = prevBucketBits;
        int n = indexBucketTableScan.getInt(BUCKET_NUM);
        int newN = (int) (n + Math.pow(2, b));

        // Add the new bucket for the splitting of the full bucket
        indexBucketTableScan.beforeFirst();
        indexBucketTableScan.insert();
        indexBucketTableScan.setInt(BUCKET_NUM, newN);
        indexBucketTableScan.setInt(BUCKET_BITS, currentBitCount);
        indexBucketTableScan.setInt(BUCKET_TUPLES, 0);
        indexBucketTableScan.setString(BUCKET_FILE_NAME, idxname + newN);
        indexBucketTableScan.beforeFirst();

        printExtensibleHashIndexInformation();

        // Reinsert the records that were marked for removal
        for (Constant k : resortList) {
            insert(k, rid);
        }

        // Insert the new value
        insert(val, rid);
    }

    /**
     * CS 4432 Project 2
     *
     * Inserts the value into an existing bucket, and increments the number of
     * tuples in that bucket.
     *
     * @param val
     *            The value to insert.
     * @param rid
     *            The RID of the value to insert.
     */
    protected void insertExistingIndexBucket(Constant val, RID rid) {
        indexBucketTableScan.setInt(BUCKET_TUPLES, indexBucketTableScan.getInt(BUCKET_TUPLES) + 1);
        TableScan tblsn = getTableByFileName(indexBucketTableScan.getString(BUCKET_FILE_NAME));

        // Insert the new value
        tblsn.beforeFirst();
        tblsn.insert();
        tblsn.setInt("block", rid.blockNumber());
        tblsn.setInt("id", rid.id());
        tblsn.setVal("dataval", val);
    }

    /**
     * CS 4432 Project 2
     *
     * Inserts the new value into a new bucket.
     *
     * @param val
     *            The value to insert.
     * @param rid
     *            The RID of the value to insert.
     * @param maskedBucketNumber
     *            The bucket number for the new bucket.
     * @param bits
     *            The number of bits to use for comparison of this bucket.
     */
    protected void insertNewIndexBucket(Constant val, RID rid, int maskedBucketNumber, int bits) {
        // Insert the new bucket
        indexBucketTableScan.insert();
        indexBucketTableScan.setInt(BUCKET_NUM, maskedBucketNumber);
        indexBucketTableScan.setInt(BUCKET_BITS, bits);
        indexBucketTableScan.setInt(BUCKET_TUPLES, 1);
        indexBucketTableScan.setString(BUCKET_FILE_NAME, idxname + maskedBucketNumber);

        // Check if the number of global bits needs to be incremented
        // TODO Check if this should be >= or just >
        if (bits >= currentBitCount) {
            currentBitCount++;
        }

        TableScan tblsn = getTableByFileName(indexBucketTableScan.getString(BUCKET_FILE_NAME));

        // Insert the record
        tblsn.beforeFirst();
        tblsn.insert();
        tblsn.setInt("block", rid.blockNumber());
        tblsn.setInt("id", rid.id());
        tblsn.setVal("dataval", val);

        // Increment the number of global bits
        currentBucketCount++;
    }

    /**
     * CS 4432 Project 2
     *
     * This method returns the TableScan for the specified table file name.
     *
     * @param fileName
     *            The file name of the table to read from disk.
     * @return a TableScan
     */
    protected TableScan getTableByFileName(String fileName) {
        TableInfo tableInfo = new TableInfo(fileName, sch);
        return new TableScan(tableInfo, tx);
    }

    /**
     * Deletes the specified record from the table scan for the bucket. The
     * method starts at the beginning of the scan, and loops through the records
     * until the specified record is found.
     *
     * @see simpledb.index.Index#delete(simpledb.query.Constant,
     *      simpledb.record.RID)
     */
    @Override
    public void delete(Constant val, RID rid) {
        beforeFirst(val);
        while (next()) {
            if (getDataRid().equals(rid)) {
                ts.delete();
                return;
            }
        }
    }

    /**
     * Closes the index by closing the current table scan.
     *
     * @see simpledb.index.Index#close()
     */
    @Override
    public void close() {
        if (ts != null) {
            ts.close();
        }
    }

    /**
     * CS 4432 Project 2
     *
     * Prints the index bucket values on call.
     */
    protected void printIndexBucketValues() {
        indexBucketTableScan.beforeFirst();
        System.out.println("\nINDEX BUCKET VALUE LISTING:\n------------------");
        while (indexBucketTableScan.next()) {
            System.out.println("NUM: " + indexBucketTableScan.getInt(BUCKET_NUM));
            System.out.println("BIT: " + indexBucketTableScan.getInt(BUCKET_BITS));
            System.out.println("CNT: " + indexBucketTableScan.getInt(BUCKET_TUPLES));
            System.out.println("FLN: " + indexBucketTableScan.getString(BUCKET_FILE_NAME));
            System.out.println("------------------");
        }
        System.out.println();
    }

    /**
     * CS 4432 Project 2
     *
     * Prints the extensible hash index information on call.
     */
    protected void printExtensibleHashIndexInformation() {
        System.out.println("\n----------------------------------");
        System.out.println("EXTENSIBLE HASH INDEX INFORMATION:");
        System.out.println("----------------------------------");
        System.out.println("Current bit count: " + currentBitCount);
        System.out.println("Current bucket count: " + currentBucketCount);
        System.out.println("Current bucket number: " + currentBucketNumber);
        System.out.println("Hash modulo value: " + HASH_MOD_VAL);
        System.out.println("----------------------------------\n");

        printIndexBucketValues();
    }

    /**
     * Returns the cost of searching an index file having the specified number
     * of blocks. The method assumes that all buckets are about the same size,
     * and so the cost is simply the size of the bucket.
     *
     * @param numblocks
     *            the number of blocks of index records
     * @param rpb
     *            the number of records per block (not used here)
     * @return the cost of traversing the index
     */
    public int searchCost(int numblocks, int rpb) {
        return numblocks / currentBucketCount;
    }
}
