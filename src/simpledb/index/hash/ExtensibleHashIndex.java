package simpledb.index.hash;

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

	protected String idxname;
	protected Schema sch;
	protected Transaction tx;
	protected Constant searchkey = null;
	protected TableScan ts = null;

	protected final String INDEX_FILENAME = "ehindexfile";

	protected final String BUCKET_NUM = "bucketnum";
	protected final String BUCKET_BITS = "buckebitse";
	protected final String BUCKET_TUPLES = "buckettuples";

	protected final int NUM_BUCKET_TUPLES = 10;

	protected Schema bucketInfo;
	protected TableInfo bucketTable;
	protected TableScan bucketScan;

	protected int bucketNum = 0;

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

		bucketInfo = new Schema();
		bucketInfo.addIntField(BUCKET_NUM);
		bucketInfo.addIntField(BUCKET_BITS);
		bucketInfo.addIntField(BUCKET_TUPLES);

		bucketTable = new TableInfo(INDEX_FILENAME, bucketInfo);
		bucketScan = new TableScan(bucketTable, tx);
	}

	@Override
	public void beforeFirst(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		int bucket = searchkey.hashCode() % currentNumberBuckets;
		bucketNum = bucket;
		String tblname = idxname + bucket;
		TableInfo ti = new TableInfo(tblname, sch);
		ts = new TableScan(ti, tx);
	}

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

				bucketScan.beforeFirst();
				while (bucketScan.next()) {
					if (bucketScan.getInt(BUCKET_NUM) == bucketNum) {
						bucketScan.setInt(BUCKET_TUPLES, bucketScan.getInt(BUCKET_TUPLES) - 1);
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

	@Override
	public void insert(Constant val, RID rid) {
		beforeFirst(val);

		bucketScan.beforeFirst();

		while (bucketScan.next()) {
			if (bucketScan.getInt(BUCKET_NUM) == bucketNum) {
				if (bucketScan.getInt(BUCKET_TUPLES) >= NUM_BUCKET_TUPLES) {
					// TODO Handle splitting buckets
					System.out.println("HANDLE SPLITTING");
					printBuckets();
					return;
				} else {
					System.out.println("Updatre count");
					bucketScan.setInt(BUCKET_TUPLES, bucketScan.getInt(BUCKET_TUPLES) + 1);
					doTableScanInsert(val, rid);
					printBuckets();
					return;
				}
			}
		}

		System.out.println("new item");
		bucketScan.insert();
		bucketScan.setInt(BUCKET_NUM, bucketNum);
		bucketScan.setInt(BUCKET_BITS, 1);
		bucketScan.setInt(BUCKET_TUPLES, 1);
		doTableScanInsert(val, rid);
		currentNumberBuckets++;

		printBuckets();
	}

	protected void doTableScanInsert(Constant val, RID rid) {
		ts.insert();
		ts.setInt("block", rid.blockNumber());
		ts.setInt("id", rid.id());
		ts.setVal("dataval", val);
	}

	protected void printBuckets() {
		bucketScan.beforeFirst();
		System.out.println("---------START---------");
		while (bucketScan.next()) {
			System.out.println(bucketScan.getInt(BUCKET_NUM));
			System.out.println(bucketScan.getInt(BUCKET_BITS));
			System.out.println(bucketScan.getInt(BUCKET_TUPLES));
			System.out.println("---------SPLIT---------");
		}
		System.out.println("---------END---------");
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
