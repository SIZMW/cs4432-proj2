package simpledb.metadata;

import java.util.Map;

import simpledb.index.IndexType;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class MetadataMgr {
    private static TableMgr tblmgr;
    private static ViewMgr viewmgr;
    private static StatMgr statmgr;
    private static IndexMgr idxmgr;

    public MetadataMgr(boolean isnew, Transaction tx) {
        tblmgr = new TableMgr(isnew, tx);
        viewmgr = new ViewMgr(isnew, tblmgr, tx);
        statmgr = new StatMgr(tblmgr, tx);
        idxmgr = new IndexMgr(isnew, tblmgr, tx);
    }

    public void createTable(String tblname, Schema sch, Transaction tx) {
        tblmgr.createTable(tblname, sch, tx);
    }

    public TableInfo getTableInfo(String tblname, Transaction tx) {
        return tblmgr.getTableInfo(tblname, tx);
    }

    public void createView(String viewname, String viewdef, Transaction tx) {
        viewmgr.createView(viewname, viewdef, tx);
    }

    public String getViewDef(String viewname, Transaction tx) {
        return viewmgr.getViewDef(viewname, tx);
    }

    /**
     * CS 4432 Project 2
     *
     * We modified the arguments passed to createIndex() to pass the index type.
     * 
     * @param type
     *            The type of index to create on the table.
     */
    public void createIndex(String idxname, String tblname, String fldname, Transaction tx, IndexType type) {
        idxmgr.createIndex(idxname, tblname, fldname, tx, type);
    }

    public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
        return idxmgr.getIndexInfo(tblname, tx);
    }

    public StatInfo getStatInfo(String tblname, TableInfo ti, Transaction tx) {
        return statmgr.getStatInfo(tblname, ti, tx);
    }
}
