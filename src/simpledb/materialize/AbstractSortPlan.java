package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

import java.util.*;

/**
 * The Abstract Plan class for the <i>sort</i> operator.
 */
public abstract class AbstractSortPlan implements Plan {
   protected Plan p;
   protected Transaction tx;
   protected Schema sch;
   protected RecordComparator comp;
   
   /**
    * Creates a sort plan for the specified query.
    * @param p the plan for the underlying query
    * @param sortfields the fields to sort by
    * @param tx the calling transaction
    */
   public AbstractSortPlan(Plan p, List<String> sortfields, Transaction tx) {
      this.p = p;
      this.tx = tx;
      sch = p.schema();
      comp = new RecordComparator(sortfields);
   }
   
   /**
    * This method is where most of the action is.
    * Up to 2 sorted temporary tables are created,
    * and are passed into SortScan for final merging.
    * @see simpledb.query.Plan#open()
    */
   public abstract Scan open();
   
   /**
    * Returns the number of blocks in the sorted table,
    * which is the same as it would be in a
    * materialized table.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * @see simpledb.query.Plan#blocksAccessed()
    */
   public abstract int blocksAccessed();
   
   /**
    * Returns the number of records in the sorted table,
    * which is the same as in the underlying query.
    * @see simpledb.query.Plan#recordsOutput()
    */
   public abstract int recordsOutput();
   
   /**
    * Returns the number of distinct field values in
    * the sorted table, which is the same as in
    * the underlying query.
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public abstract int distinctValues(String fldname);
   
   /**
    * Returns the schema of the sorted table, which
    * is the same as in the underlying query.
    * @see simpledb.query.Plan#schema()
    */
   public abstract Schema schema();
   
   protected abstract List<TempTable> splitIntoRuns(Scan src);
   
   protected abstract List<TempTable> doAMergeIteration(List<TempTable> runs);
   
   protected abstract TempTable mergeTwoRuns(TempTable p1, TempTable p2);
   
   protected abstract boolean copy(Scan src, UpdateScan dest);
}
