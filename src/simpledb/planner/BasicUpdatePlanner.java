package simpledb.planner;

import java.util.Iterator;

import simpledb.parse.CreateIndexData;
import simpledb.parse.CreateTableData;
import simpledb.parse.CreateViewData;
import simpledb.parse.DeleteData;
import simpledb.parse.InsertData;
import simpledb.parse.ModifyData;
import simpledb.query.Constant;
import simpledb.query.Plan;
import simpledb.query.SelectPlan;
import simpledb.query.TablePlan;
import simpledb.query.UpdateScan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * The basic planner for SQL update statements.
 *
 * @author sciore
 */
public class BasicUpdatePlanner implements UpdatePlanner {

	@Override
	public int executeDelete(DeleteData data, Transaction tx) {
		Plan p = new TablePlan(data.tableName(), tx);
		p = new SelectPlan(p, data.pred());
		UpdateScan us = (UpdateScan) p.open();
		int count = 0;
		while (us.next()) {
			us.delete();
			count++;
		}
		us.close();
		return count;
	}

	@Override
	public int executeModify(ModifyData data, Transaction tx) {
		Plan p = new TablePlan(data.tableName(), tx);
		p = new SelectPlan(p, data.pred());
		UpdateScan us = (UpdateScan) p.open();
		int count = 0;
		while (us.next()) {
			Constant val = data.newValue().evaluate(us);
			us.setVal(data.targetField(), val);
			count++;
		}
		us.close();
		return count;
	}

	@Override
	public int executeInsert(InsertData data, Transaction tx) {
		Plan p = new TablePlan(data.tableName(), tx);
		UpdateScan us = (UpdateScan) p.open();
		us.insert();
		Iterator<Constant> iter = data.vals().iterator();
		for (String fldname : data.fields()) {
			Constant val = iter.next();
			us.setVal(fldname, val);
		}
		us.close();
		return 1;
	}

	@Override
	public int executeCreateTable(CreateTableData data, Transaction tx) {
		SimpleDB.mdMgr().createTable(data.tableName(), data.newSchema(), tx);
		return 0;
	}

	@Override
	public int executeCreateView(CreateViewData data, Transaction tx) {
		SimpleDB.mdMgr().createView(data.viewName(), data.viewDef(), tx);
		return 0;
	}

	/**
	 * CS 4432 Project 2
	 *
	 * We modified the arguments passed to createIndex() to pass the index type.
	 *
	 * (non-Javadoc)
	 *
	 * @see simpledb.planner.UpdatePlanner#executeCreateIndex(simpledb.parse.CreateIndexData,
	 *      simpledb.tx.Transaction)
	 */
	@Override
	public int executeCreateIndex(CreateIndexData data, Transaction tx) {
		SimpleDB.mdMgr().createIndex(data.indexName(), data.tableName(), data.fieldName(), tx, data.indexType());
		return 0;
	}
}
