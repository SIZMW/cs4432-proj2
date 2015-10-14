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
 * An extensible hash implementation of the Index interface. Each bucket is
 * implemented as a file of index records, and the number of buckets can grow or
 * shrink.
 *
 * TODO This entire class
 *
 * @author Aditya Nivarthi (anivarthi)
 */
public class ExtensibleHashIndex implements Index {

	protected int currentNumberBuckets = 2;

	// Hash information
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

	// Maximum number of tuples in an index bucket
	protected final int NUM_BUCKET_TUPLES = 2;

	// Index information
	protected Schema indexBucketSchema;
	protected TableInfo indexBucketTableInfo;
	protected TableScan indexBucketTableScan;

	// Bucket number for current value
	protected int currentBucketNumber = 0;

	// Hash value for modulo
	protected static final int HASH_MOD = 1610612741;

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

		indexBucketTableInfo = new TableInfo(INDEX_FILENAME, indexBucketSchema);
		indexBucketTableScan = new TableScan(indexBucketTableInfo, tx);

		indexBucketTableScan.beforeFirst();
	}

	/**
	 * (non-Javadoc)
	 *
	 * @see simpledb.index.Index#beforeFirst(simpledb.query.Constant)
	 */
	@Override
	public void beforeFirst(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		int bucket = searchkey.hashCode() % HASH_MOD;

		System.out.println("BUCKET NUM: " + bucket);

		currentBucketNumber = bucket;
		String tblname = idxname + bucket;
		TableInfo ti = new TableInfo(tblname, sch);
		ts = new TableScan(ti, tx);
	}

	/**
	 * (non-Javadoc)
	 *
	 * @see simpledb.index.Index#close()
	 */
	@Override
	public void close() {
		if (ts != null) {
			ts.close();
		}
	}

	@Override
	public void delete(Constant val, RID rid) {
		beforeFirst(val);
		while (next()) {
			if (getDataRid().equals(rid)) {
				indexBucketTableScan.beforeFirst();
				while (indexBucketTableScan.next()) {
					int bucketNumberMask = indexBucketTableScan.getInt(BUCKET_NUM)
							& getMask(indexBucketTableScan.getInt(BUCKET_BITS));

					int currentBucketNumberMask = currentBucketNumber
							& getMask(indexBucketTableScan.getInt(BUCKET_BITS));

					if (bucketNumberMask == currentBucketNumberMask) {
						indexBucketTableScan.setInt(BUCKET_TUPLES, indexBucketTableScan.getInt(BUCKET_TUPLES) - 1);
					}
				}

				ts.delete();
				return;
			}
		}
	}

	@Override
	public RID getDataRid() {
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}

	protected int getMask(int bits) {
		int sum = 0;
		for (int i = bits - 1; i >= 0; i--) {
			sum += Math.pow(10, i);
		}
		return sum;
	}

	@Override
	public void insert(Constant val, RID rid) {
		beforeFirst(val);
		indexBucketTableScan.beforeFirst();
		while (indexBucketTableScan.next()) {
			int bucketNumberMask = indexBucketTableScan.getInt(BUCKET_NUM)
					& getMask(indexBucketTableScan.getInt(BUCKET_BITS));

			int currentBucketNumberMask = currentBucketNumber & getMask(indexBucketTableScan.getInt(BUCKET_BITS));

			System.out.println("CURRENT BUCKET NUMBER MASKED: " + currentBucketNumberMask);
			System.out.println("BUCKET NUMBER MASKED: " + bucketNumberMask);

			if (bucketNumberMask == currentBucketNumberMask) {
				if (indexBucketTableScan.getInt(BUCKET_TUPLES) > NUM_BUCKET_TUPLES) {
					// TODO Fix resorting all the records when increasing bits
					getRecordsToMove(val, currentBucketNumber);
					insert(val, rid);
					return;
				} else {
					insertIntoExistingIndexBucket(val, rid);
					printIndexBucketValues();
					return;
				}
			}
		}

		// Insert new index and table record, with bit value of 1 for index
		insertNewIndexAndTableRecord(val, rid, 1);
		printIndexBucketValues();
	}

	protected void insertIntoExistingIndexBucket(Constant val, RID rid) {
		System.out.println("UPDATE COUNT EXISTING BIT INDEX");
		indexBucketTableScan.setInt(BUCKET_TUPLES, indexBucketTableScan.getInt(BUCKET_TUPLES) + 1);

		ts.insert();
		ts.setInt("block", rid.blockNumber());
		ts.setInt("id", rid.id());
		ts.setVal("dataval", val);
	}

	protected void getRecordsToMove(Constant val, int currentBucket) {
		int b = indexBucketTableScan.getInt(BUCKET_BITS);
		int n = indexBucketTableScan.getInt(BUCKET_NUM);

		int newN = (int) (n + Math.pow(2, b));

		indexBucketTableScan.insert();
		indexBucketTableScan.setInt(BUCKET_NUM, newN);
		indexBucketTableScan.setInt(BUCKET_BITS, b + 1);
		indexBucketTableScan.setInt(BUCKET_TUPLES, 0);

		indexBucketTableScan.setInt(BUCKET_BITS, indexBucketTableScan.getInt(BUCKET_BITS) + 1);

		beforeFirst(val);

		List<Constant> keyList = new ArrayList<Constant>();

		// TODO figure out why this is false
		System.out.println(ts.next());
		while (ts.next()) {
			Constant key = ts.getVal("dataval");
			int bucket = key.hashCode() % HASH_MOD;

			int checkBucketNumMask = indexBucketTableScan.getInt(BUCKET_NUM)
					& getMask(indexBucketTableScan.getInt(BUCKET_BITS));

			int bucketNumMask = bucket & getMask(indexBucketTableScan.getInt(BUCKET_BITS));

			if (bucketNumMask != checkBucketNumMask) {
				keyList.add(key);
				System.out.println("DELETE RECORD FOR REINSERT");
				ts.delete();
			}
		}

		for (Constant k : keyList) {
			System.out.println("REINSERT VALUES");
			insert(k, ts.getRid());
		}
	}

	protected void insertNewIndexAndTableRecord(Constant val, RID rid, int bits) {
		int masked = currentBucketNumber & getMask(bits);
		System.out.println("NEW BIT ENTRY IN INDEX");
		indexBucketTableScan.insert();
		indexBucketTableScan.setInt(BUCKET_NUM, masked);
		indexBucketTableScan.setInt(BUCKET_BITS, bits);
		indexBucketTableScan.setInt(BUCKET_TUPLES, 1);

		ts.insert();
		ts.setInt("block", rid.blockNumber());
		ts.setInt("id", rid.id());
		ts.setVal("dataval", val);

		currentNumberBuckets++;
	}

	protected void printIndexBucketValues() {
		System.out.println("\nINDEX BUCKET VALUE LISTING:\n------------------");
		indexBucketTableScan.beforeFirst();
		while (indexBucketTableScan.next()) {
			System.out.println("NUM: " + indexBucketTableScan.getInt(BUCKET_NUM));
			System.out.println("BIT: " + indexBucketTableScan.getInt(BUCKET_BITS));
			System.out.println("CNT: " + indexBucketTableScan.getInt(BUCKET_TUPLES));
			System.out.println("------------------");
		}
		System.out.println();
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

	public int searchCost(int numblocks, int rpb) {
		return numblocks / currentNumberBuckets;
	}
}
