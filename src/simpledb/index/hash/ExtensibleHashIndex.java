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
	}

	@Override
	public void beforeFirst(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		int bucket = searchkey.hashCode() % currentNumberBuckets;
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
	public void insert(Constant dataval, RID datarid) {
		// TODO Auto-generated method stub
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
