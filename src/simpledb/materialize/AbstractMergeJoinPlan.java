package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import java.util.*;

/**
 * The Plan abstract class for the <i>mergejoin</i> operator.
 */
public abstract class AbstractMergeJoinPlan implements Plan {
   
   /**
    * Creates a mergejoin plan for the two specified queries.
    * The RHS must be materialized after it is sorted, 
    * in order to deal with possible duplicates.
    * @param p1 the LHS query plan
    * @param p2 the RHS query plan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    * @param tx the calling transaction
    */
   public AbstractMergeJoinPlan() {}
   
   /** The method first sorts its two underlying scans
     * on their join field. It then returns a mergejoin scan
     * of the two sorted table scans.
     * @see simpledb.query.Plan#open()
     */
   public abstract Scan open();
   
   /**
    * Returns the number of block acceses required to
    * mergejoin the sorted tables.
    * Since a mergejoin can be preformed with a single
    * pass through each table, the method returns
    * the sum of the block accesses of the 
    * materialized sorted tables.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * @see simpledb.query.Plan#blocksAccessed()
    */
   public abstract int blocksAccessed();
   
   /**
    * Returns the number of records in the join.
    * Assuming uniform distribution, the formula is:
    * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
    * @see simpledb.query.Plan#recordsOutput()
    */
   public abstract int recordsOutput();
   
   /**
    * Estimates the distinct number of field values in the join.
    * Since the join does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public abstract int distinctValues(String fldname);
   
   /**
    * Returns the schema of the join,
    * which is the union of the schemas of the underlying queries.
    * @see simpledb.query.Plan#schema()
    */
   public abstract Schema schema();
}

