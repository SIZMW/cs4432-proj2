package simpledb.materialize;

import simpledb.query.*;

/**
 * The Abstract Scan class for the <i>mergejoin</i> operator.
 */
public abstract class AbstractMergeJoinScan implements Scan {
   protected Scan s1;
   protected SortScan s2;
   protected String fldname1, fldname2;
   protected Constant joinval = null;
   
   /**
    * Creates a mergejoin scan for the two underlying sorted scans.
    * @param s1 the LHS sorted scan
    * @param s2 the RHS sorted scan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    */
   public AbstractMergeJoinScan(Scan s1, SortScan s2, String fldname1, String fldname2) {
      this.s1 = s1;
      this.s2 = s2;
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      beforeFirst();
   }
   
   /**
    * Positions the scan before the first record,
    * by positioning each underlying scan before
    * their first records.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public abstract void beforeFirst();
   
   /**
    * Closes the scan by closing the two underlying scans.
    * @see simpledb.query.Scan#close()
    */
   public abstract void close();
   
   /**
    * Moves to the next record.  This is where the action is.
    * <P>
    * If the next RHS record has the same join value,
    * then move to it.
    * Otherwise, if the next LHS record has the same join value,
    * then reposition the RHS scan back to the first record
    * having that join value.
    * Otherwise, repeatedly move the scan having the smallest
    * value until a common join value is found.
    * When one of the scans runs out of records, return false.
    * @see simpledb.query.Scan#next()
    */
   public abstract boolean next();
   
   /** 
    * Returns the value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public abstract Constant getVal(String fldname);
   
   /** 
    * Returns the integer value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public abstract int getInt(String fldname);
   
   /** 
    * Returns the string value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public abstract String getString(String fldname);
   
   /**
    * Returns true if the specified field is in
    * either of the underlying scans.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public abstract boolean hasField(String fldname);
}

