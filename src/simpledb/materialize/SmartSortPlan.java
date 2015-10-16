package simpledb.materialize;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.query.Constant;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * The Smart Plan class for the <i>sort</i> operator.
 */
public class SmartSortPlan extends AbstractSortPlan {
    private List<String> sortFields;

    /**
     * Creates a sort plan for the specified query.
     *
     * @param p
     *            the plan for the underlying query
     * @param sortfields
     *            the fields to sort by
     * @param tx
     *            the calling transaction
     */
    public SmartSortPlan(Plan p, List<String> sortfields, Transaction tx) {
        super(p, sortfields, tx);

        sortFields = sortfields;
    }

    /**
     * This method is where most of the action is. Up to 2 sorted temporary
     * tables are created, and are passed into SortScan for final merging.
     *
     * @see simpledb.query.Plan#open()
     */
    @Override
    public Scan open() {
        Boolean sorted = false;
        String tableName = "Temp Table";

        if (p instanceof TablePlan) {
            TablePlan tp = (TablePlan) p;
            tableName = tp.getTableInfo().tableName();
            sorted = tp.getTableInfo().getSorted();
<<<<<<< HEAD
            if (tp.getTableInfo().getSortFields().size() == this.sortFields.size()) {
                for (int i = 0; i < this.sortFields.size(); i++) {
                    if (!(tp.getTableInfo().getSortFields().get(i).equals(this.sortFields.get(i)))) {
                        System.out.println("Sorted fields of table and query do not match");
=======
            if (tp.getTableInfo().getSortFields().size() == sortFields.size()) {
                for (int i = 0; i < sortFields.size(); i++) {
                    if (!(tp.getTableInfo().getSortFields().get(i).equals(sortFields.get(i)))) {
>>>>>>> fdaa78f69b53ec7c4910fcea82239023d155c880
                        sorted = false;
                    }
                }
            } else {
<<<<<<< HEAD
                System.out.println("Length of sorted fields of table and query do not match");
=======
>>>>>>> fdaa78f69b53ec7c4910fcea82239023d155c880
                sorted = false;
            }
        }

        List<TempTable> runs;
        // If the table is already sorted, it isn't necessary to sort again.
        if (!sorted) {
            System.out.println("Sorting table " + tableName);
            
            Scan src = p.open();
            runs = splitIntoRuns(src);
            src.close();
            
            while (runs.size() > 2) {
                runs = doAMergeIteration(runs);
            }
        } else {
            System.out.println("Table already sorted " + tableName);
            
            runs = new ArrayList<TempTable>();
            
            TempTable currenttemp = new TempTable(sch, tx);
            
            runs.add(currenttemp);

            UpdateScan destination = currenttemp.open();
            Scan src = p.open();

            src.beforeFirst();
            destination.beforeFirst();
            int i = 0;
            while (src.next()) {
                destination.insert();
                System.out.println(i);
                for (String fldname : sch.fields()) {
                    System.out.println("B");
                    destination.setVal(fldname, src.getVal(fldname));
                    System.out.println("C");
                }

                if (comp.compare(src, destination) < 0) {
                    System.out.println("A");
                    // start a new run
                    destination.close();
                    currenttemp = new TempTable(sch, tx);
                    runs.add(currenttemp);
                    destination = currenttemp.open();
                }
                i++;
            }
            destination.close();
            src.close();
        }

        // Write the new records to the table
        if (p instanceof TablePlan) {
            TablePlan tp = (TablePlan) p;
<<<<<<< HEAD

            String fileName = tp.getTableInfo().fileName();

            System.out.println("Copying records to " + tableName);

            UpdateScan destination = (UpdateScan) tp.open();
            Scan sortedScan = new SortScan(runs, comp);

            sortedScan.beforeFirst();
            destination.beforeFirst();
            int i = 0;
            while(sortedScan.next() && destination.next()) {
                // destination.insert();
                // Overwrite the values
                for (String fldname : sch.fields()) {
                    destination.setVal(fldname, sortedScan.getVal(fldname));
                    System.out.println(i + ": " + fldname + " " + sortedScan.getVal(fldname));
                }
                i++;
            }

            System.out.println("Done copying records");
            sortedScan.close();
            destination.close();

            // Modify table metadata
            TablePlan mdplan = new TablePlan("tblcat", tx);
            UpdateScan mdscan = (UpdateScan) mdplan.open();
            mdscan.beforeFirst();
            while (mdscan.next()) {
                if (mdscan.getString("tblname").equals(tableName)) {
                    mdscan.setString("sortname", sortFields.get(0));
                }
            }
            mdscan.close();
=======
            tp.getTableInfo().setSorted(true);
            tp.getTableInfo().setSortFields(sortFields);
>>>>>>> fdaa78f69b53ec7c4910fcea82239023d155c880
        }

        // Write temp tables generated to the file blocks
        // get some kind of page.write()

        return new SortScan(runs, comp);
    }

    /**
     * Returns the number of blocks in the sorted table, which is the same as it
     * would be in a materialized table. It does <i>not</i> include the one-time
     * cost of materializing and sorting the records.
     *
     * @see simpledb.query.Plan#blocksAccessed()
     */
    @Override
    public int blocksAccessed() {
        // does not include the one-time cost of sorting
        Plan mp = new MaterializePlan(p, tx); // not opened; just for analysis
        return mp.blocksAccessed();
    }

    /**
     * Returns the number of records in the sorted table, which is the same as
     * in the underlying query.
     *
     * @see simpledb.query.Plan#recordsOutput()
     */
    @Override
    public int recordsOutput() {
        return p.recordsOutput();
    }

    /**
     * Returns the number of distinct field values in the sorted table, which is
     * the same as in the underlying query.
     *
     * @see simpledb.query.Plan#distinctValues(java.lang.String)
     */
    @Override
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    /**
     * Returns the schema of the sorted table, which is the same as in the
     * underlying query.
     *
     * @see simpledb.query.Plan#schema()
     */
    @Override
    public Schema schema() {
        return sch;
    }

    @Override
    protected List<TempTable> splitIntoRuns(Scan src) {
        List<TempTable> temps = new ArrayList<TempTable>();
        src.beforeFirst();
        if (!src.next()) {
            return temps;
        }
        TempTable currenttemp = new TempTable(sch, tx);
        temps.add(currenttemp);
        UpdateScan currentscan = currenttemp.open();
        while (copy(src, currentscan)) {
            if (comp.compare(src, currentscan) < 0) {
                // start a new run
                currentscan.close();
                currenttemp = new TempTable(sch, tx);
                temps.add(currenttemp);
                currentscan = currenttemp.open();
            }
        }
        currentscan.close();
        return temps;
    }

    @Override
    protected List<TempTable> doAMergeIteration(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<TempTable>();
        while (runs.size() > 1) {
            TempTable p1 = runs.remove(0);
            TempTable p2 = runs.remove(0);
            result.add(mergeTwoRuns(p1, p2));
        }
        if (runs.size() == 1) {
            result.add(runs.get(0));
        }
        return result;
    }

    @Override
    protected TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
        Scan src1 = p1.open();
        Scan src2 = p2.open();
        TempTable result = new TempTable(sch, tx);
        UpdateScan dest = result.open();

        boolean hasmore1 = src1.next();
        boolean hasmore2 = src2.next();
        while (hasmore1 && hasmore2) {
            if (comp.compare(src1, src2) < 0) {
                hasmore1 = copy(src1, dest);
            } else {
                hasmore2 = copy(src2, dest);
            }
        }

        if (hasmore1) {
            while (hasmore1) {
                hasmore1 = copy(src1, dest);
            }
        } else {
            while (hasmore2) {
                hasmore2 = copy(src2, dest);
            }
        }
        src1.close();
        src2.close();
        dest.close();
        return result;
    }

    @Override
    protected boolean copy(Scan src, UpdateScan dest) {
        dest.insert();
        for (String fldname : sch.fields()) {
            dest.setVal(fldname, src.getVal(fldname));
        }
        return src.next();
    }
}
