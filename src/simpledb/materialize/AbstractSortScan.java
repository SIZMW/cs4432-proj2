package simpledb.materialize;

import simpledb.record.RID;
import simpledb.query.*;
import java.util.*;

/**
 * The Abstract Scan class for the <i>sort</i> operator.
 */
public abstract class AbstractSortScan implements Scan {
   protected UpdateScan s1, s2=null, currentscan=null;
   protected RecordComparator comp;
   protected boolean hasmore1, hasmore2=false;
   protected List<RID> savedposition;
   
   /**
    * Creates a sort scan, given a list of 1 or 2 runs.
    * If there is only 1 run, then s2 will be null and
    * hasmore2 will be false.
    * @param runs the list of runs
    * @param comp the record comparator
    */
   public AbstractSortScan(List<TempTable> runs, RecordComparator comp) {
      this.comp = comp;
      s1 = (UpdateScan) runs.get(0).open();
      hasmore1 = s1.next();
      if (runs.size() > 1) {
         s2 = (UpdateScan) runs.get(1).open();
         hasmore2 = s2.next();
      }
   }
   
   /**
    * Positions the scan before the first record in sorted order.
    * Internally, it moves to the first record of each underlying scan.
    * The variable currentscan is set to null, indicating that there is
    * no current scan.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public abstract void beforeFirst();
   
   /**
    * Moves to the next record in sorted order.
    * First, the current scan is moved to the next record.
    * Then the lowest record of the two scans is found, and that
    * scan is chosen to be the new current scan.
    * @see simpledb.query.Scan#next()
    */
   public abstract boolean next();
   
   /**
    * Closes the two underlying scans.
    * @see simpledb.query.Scan#close()
    */
   public abstract void close();
   
   /**
    * Gets the Constant value of the specified field
    * of the current scan.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public abstract Constant getVal(String fldname);
   
   /**
    * Gets the integer value of the specified field
    * of the current scan.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public abstract int getInt(String fldname);
   
   /**
    * Gets the string value of the specified field
    * of the current scan.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public abstract String getString(String fldname);
   
   /**
    * Returns true if the specified field is in the current scan.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public abstract boolean hasField(String fldname);
   
   /**
    * Saves the position of the current record,
    * so that it can be restored at a later time.
    */
   public abstract void savePosition();
   
   /**
    * Moves the scan to its previously-saved position.
    */
   public abstract void restorePosition();
}
