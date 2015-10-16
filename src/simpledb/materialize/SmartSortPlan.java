package simpledb.materialize;

import java.util.ArrayList;
import java.util.List;

import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
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

        if (p instanceof TablePlan) {
            TablePlan tp = (TablePlan) p;
            sorted = tp.getTableInfo().getSorted();
            if (tp.getTableInfo().getSortFields().size() == sortFields.size()) {
                for (int i = 0; i < sortFields.size(); i++) {
                    if (!(tp.getTableInfo().getSortFields().get(i).equals(sortFields.get(i)))) {
                        sorted = false;
                    }
                }
            } else {
                sorted = false;
            }
        }

        Scan src = p.open();
        List<TempTable> runs = new ArrayList<TempTable>();
        // If the table is already sorted, it isn't necessary to sort again.
        if (!sorted) {
            runs = splitIntoRuns(src);
            while (runs.size() > 2) {
                runs = doAMergeIteration(runs);
            }
        } else {
            runs = new ArrayList<TempTable>();
            TempTable currenttemp = new TempTable(sch, tx);
            runs.add(currenttemp);
            UpdateScan destination = currenttemp.open();
            boolean hasMore = copy(src, destination);
            while (hasMore) {
                hasMore = copy(src, destination);
            }
        }
        src.close();

        if (p instanceof TablePlan) {
            TablePlan tp = (TablePlan) p;
            tp.getTableInfo().setSorted(true);
            tp.getTableInfo().setSortFields(sortFields);
        }

        // Write temp tables generated to the file blocks
        // get some kind of page.write()

        return new SmartSortScan(runs, comp);
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
