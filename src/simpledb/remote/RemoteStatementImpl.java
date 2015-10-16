package simpledb.remote;

import java.util.logging.Level;

import simpledb.tx.Transaction;
import simpledb.query.Plan;
import simpledb.server.SimpleDB;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * The RMI server-side implementation of RemoteStatement.
 * @author Edward Sciore
 */
@SuppressWarnings("serial")
class RemoteStatementImpl extends UnicastRemoteObject implements RemoteStatement {
   private RemoteConnectionImpl rconn;
   
   public RemoteStatementImpl(RemoteConnectionImpl rconn) throws RemoteException {
      this.rconn = rconn;
   }
   
    /**
     * Executes the specified SQL query string.
     * The method calls the query planner to create a plan
     * for the query. It then sends the plan to the
     * RemoteResultSetImpl constructor for processing.
     * @see simpledb.remote.RemoteStatement#executeQuery(java.lang.String)
     */
    public RemoteResultSet executeQuery(String qry) throws RemoteException {
        try {
            // Begin performance logging
            long initIos = SimpleDB.fileMgr().getIos();
            long startTime = System.nanoTime();
            
            Transaction tx = rconn.getTransaction();
            Plan pln = SimpleDB.planner().createQueryPlan(qry, tx);

            // Report performance logging
            float elapsedTime = ((float)(System.nanoTime() - startTime)/1000)/1000;
            long iosDone = SimpleDB.fileMgr().getIos() - initIos;
            SimpleDB.getLogger().log(Level.INFO, 
                "Query Executed" +
                "\n\t" + qry +
                "\n\tTime elapsed: " + elapsedTime + " ms" + 
                "\n\tIOs done: " + iosDone);
            return new RemoteResultSetImpl(pln, rconn);
        }
        catch(RuntimeException e) {
            rconn.rollback();
            throw e;
        }
    }
   
    /**
     * Executes the specified SQL update command.
     * The method sends the command to the update planner,
     * which executes it.
     * @see simpledb.remote.RemoteStatement#executeUpdate(java.lang.String)
     */
    public int executeUpdate(String cmd) throws RemoteException {
        try {
            // Begin performance logging
            long initIos = SimpleDB.fileMgr().getIos();
            long startTime = System.nanoTime();

            Transaction tx = rconn.getTransaction();
            int result = SimpleDB.planner().executeUpdate(cmd, tx);
            rconn.commit();
            
            // Report performance logging
            float elapsedTime = ((float)(System.nanoTime() - startTime)/1000)/1000;
            long iosDone = SimpleDB.fileMgr().getIos() - initIos;
            SimpleDB.getLogger().log(Level.INFO, 
                "Update Executed" +
                "\n\t" + cmd +
                "\n\tTime elapsed: " + elapsedTime + " ms" + 
                "\n\tIOs done: " + iosDone);
            
            return result;
        }
        catch(RuntimeException e) {
            rconn.rollback();
            throw e;
        }
    }
}
