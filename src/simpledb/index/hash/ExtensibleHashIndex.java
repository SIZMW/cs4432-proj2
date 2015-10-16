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
	protected final int NUM_BUCKET_TUPLES = 1000;

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

		indexBucketSchema = new Schema();
		indexBucketSchema.addIntField(BUCKET_NUM);
		indexBucketSchema.addIntField(BUCKET_BITS);
		indexBucketSchema.addIntField(BUCKET_TUPLES);
		indexBucketSchema.addStringField(BUCKET_FILE_NAME, 10);

		indexBucketTableInfo = new TableInfo(INDEX_FILENAME, indexBucketSchema);
		indexBucketTableScan = new TableScan(indexBucketTableInfo, tx);

		indexBucketTableScan.beforeFirst();
	}

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

		System.out.println("MASK FUNCTION: " + sum);
		return sum;
	}

	/**
	 * Inserts a new record into the table scan for the bucket.
	 *
	 * @see simpledb.index.Index#insert(simpledb.query.Constant,
	 *      simpledb.record.RID)
	 */
	@Override
	public void insert(Constant val, RID rid) {
		beforeFirst(val);
		indexBucketTableScan.beforeFirst();

		while (indexBucketTableScan.next()) {
			int bucketNumberMask = indexBucketTableScan.getInt(BUCKET_NUM)
					& getMask(indexBucketTableScan.getInt(BUCKET_BITS));

			int currentBucketNumberMask = currentBucketNumber & getMask(indexBucketTableScan.getInt(BUCKET_BITS));

			System.out.println("CURRENT BUCKET NUMBER: " + currentBucketNumber);
			System.out.println("BUCKET NUMBER: " + indexBucketTableScan.getInt(BUCKET_NUM));
			System.out.println("CURRENT BUCKET NUMBER MASKED: " + currentBucketNumberMask);
			System.out.println("BUCKET NUMBER MASKED: " + bucketNumberMask);
			System.out.println("HASH VALUE: " + currentBucketNumber);
			System.out.println("MASK VALUE: " + getMask(indexBucketTableScan.getInt(BUCKET_BITS)));

			if (bucketNumberMask == currentBucketNumberMask) {
				if (indexBucketTableScan.getInt(BUCKET_TUPLES) >= NUM_BUCKET_TUPLES) {
					reorganizeBucketRecords(val, rid);
					return;
				} else {
					insertExistingIndexBucket(val, rid);
					printIndexBucketValues();
					return;
				}
			}
		}

		System.out.println("CURR: " + currentBucketNumber + " " + getMask(currentBitCount) + " " + currentBitCount);
		insertNewIndexBucket(val, rid, currentBucketNumber & getMask(currentBitCount), currentBitCount);
		printIndexBucketValues();
	}

	protected void reorganizeBucketRecords(Constant val, RID rid) {
		System.out.println("HANDLE SPLITTING");

		int prevBucketBits = indexBucketTableScan.getInt(BUCKET_BITS);
		indexBucketTableScan.setInt(BUCKET_BITS, indexBucketTableScan.getInt(BUCKET_BITS) + 1);

		if (indexBucketTableScan.getInt(BUCKET_BITS) >= currentBitCount) {
			currentBitCount++;
		}

		String fileName = indexBucketTableScan.getString(BUCKET_FILE_NAME);
		TableScan tblsn = getTableByFileName(fileName);
		tblsn.beforeFirst();

		List<Constant> resortList = new ArrayList<Constant>();

		System.out.println("OPEN TABLE SCAN");
		while (tblsn.next()) {
			Constant key = tblsn.getVal("dataval");
			int bucket = key.hashCode() % HASH_MOD_VAL;

			System.out.println("LOOP OVER TABLE SCAN: " + bucket + " " + key.hashCode());

			int checkBucketNumMask = indexBucketTableScan.getInt(BUCKET_NUM)
					& getMask(indexBucketTableScan.getInt(BUCKET_BITS));

			int bucketNumMask = bucket & getMask(indexBucketTableScan.getInt(BUCKET_BITS));

			if (bucketNumMask != checkBucketNumMask) {
				resortList.add(key);
				System.out.println("DELETE RECORD FOR REINSERT: " + bucket + " " + key.hashCode());
				indexBucketTableScan.setInt(BUCKET_TUPLES, indexBucketTableScan.getInt(BUCKET_TUPLES) - 1);
				tblsn.delete();
			}
		}

		int b = prevBucketBits;
		int n = indexBucketTableScan.getInt(BUCKET_NUM);
		int newN = (int) (n + Math.pow(2, b));

		System.out.println("b: " + b);
		System.out.println("n: " + n);
		System.out.println("newN: " + newN);

		indexBucketTableScan.beforeFirst();
		indexBucketTableScan.insert();
		indexBucketTableScan.setInt(BUCKET_NUM, newN);
		indexBucketTableScan.setInt(BUCKET_BITS, currentBitCount);
		indexBucketTableScan.setInt(BUCKET_TUPLES, 0);
		indexBucketTableScan.setString(BUCKET_FILE_NAME, idxname + newN);
		indexBucketTableScan.beforeFirst();

		printIndexBucketValues();

		for (Constant k : resortList) {
			System.out.println("REINSERT VALUE: " + k.hashCode());
			insert(k, rid);
		}

		insert(val, rid);
	}

	protected void insertExistingIndexBucket(Constant val, RID rid) {
		indexBucketTableScan.setInt(BUCKET_TUPLES, indexBucketTableScan.getInt(BUCKET_TUPLES) + 1);
		TableScan tblsn = getTableByFileName(indexBucketTableScan.getString(BUCKET_FILE_NAME));

		System.out.println("INSERT EXISTING BUCKET: " + indexBucketTableScan.getInt(BUCKET_NUM));

		tblsn.beforeFirst();
		tblsn.insert();
		tblsn.setInt("block", rid.blockNumber());
		tblsn.setInt("id", rid.id());
		tblsn.setVal("dataval", val);
	}

	protected void insertNewIndexBucket(Constant val, RID rid, int maskedBucketNumber, int bits) {
		indexBucketTableScan.insert();
		indexBucketTableScan.setInt(BUCKET_NUM, maskedBucketNumber);
		indexBucketTableScan.setInt(BUCKET_BITS, bits);
		indexBucketTableScan.setInt(BUCKET_TUPLES, 1);
		indexBucketTableScan.setString(BUCKET_FILE_NAME, idxname + maskedBucketNumber);

		if (bits >= currentBitCount) {
			currentBitCount++;
		}

		System.out
				.println("INSERT NEW BUCKET: " + maskedBucketNumber + " " + bits + " " + idxname + maskedBucketNumber);

		TableScan tblsn = getTableByFileName(indexBucketTableScan.getString(BUCKET_FILE_NAME));

		tblsn.beforeFirst();
		tblsn.insert();
		tblsn.setInt("block", rid.blockNumber());
		tblsn.setInt("id", rid.id());
		tblsn.setVal("dataval", val);

		currentBucketCount++;
	}

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
