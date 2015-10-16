package simpledb.record;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import java.util.*;

/**
 * The metadata about a table and its records.
 * @author Edward Sciore
 */
public class TableInfo {
   private Schema schema;
   private Map<String,Integer> offsets;
   private int recordlen;
   private String tblname;
   private boolean sorted;
   private List<String> sortFields;
   
    /**
     * Creates a TableInfo object, given a table name
     * and schema. The constructor calculates the
     * physical offset of each field.
     * This constructor is used when a table is created. 
     * @param tblname the name of the table
     * @param schema the schema of the table's records
     */
    public TableInfo(
        String tblname, 
        Schema schema
        ) {
        this(tblname, schema, new HashMap<String,Integer>(), 0, false, new ArrayList<String>());
        for (String fldname : this.schema.fields()) {
            this.offsets.put(fldname, this.recordlen);
            this.recordlen += lengthInBytes(fldname);
        }
    }

    /**
     * Creates a TableInfo object from the 
     * specified metadata.
     * This constructor is used when the metadata
     * is retrieved from the catalog.
     * @param tblname the name of the table
     * @param schema the schema of the table's records
     * @param offsets the already-calculated offsets of the fields within a record
     * @param recordlen the already-calculated length of each record
     */
    public TableInfo(
        String tblname, 
        Schema schema, 
        Map<String,Integer> offsets, 
        int recordlen
        ) {
        this(tblname, schema, offsets, recordlen, false, new ArrayList<String>());
    }

    /**
     * Creates a TableInfo object from the 
     * specified metadata.
     * This constructor is used when the metadata
     * is retrieved from the catalog.
     * @param tblname the name of the table
     * @param schema the schema of the table's records
     * @param offsets the already-calculated offsets of the fields within a record
     * @param recordlen the already-calculated length of each record
     */
    public TableInfo(
        String tblname, 
        Schema schema, 
        Map<String,Integer> offsets, 
        int recordlen, 
        boolean sorted,
        List<String> sortFields
        ) {
        this.tblname   = tblname;
        this.schema    = schema;
        this.offsets   = offsets;
        this.recordlen = recordlen;
        this.sorted = sorted;
        this.sortFields = sortFields;
    }
   
   /**
    * Returns the name assigned to this table.
    * @return the name of the table
    */
   public String tableName() {
      return tblname;
   }

   /**
    * Returns the filename assigned to this table.
    * Currently, the filename is the table name
    * followed by ".tbl".
    * @return the name of the file assigned to the table
    */
   public String fileName() {
      return tblname + ".tbl";
   }
   
   /**
    * Returns the schema of the table's records
    * @return the table's record schema
    */
   public Schema schema() {
      return schema;
   }
   
   /**
    * Returns the offset of a specified field within a record
    * @param fldname the name of the field
    * @return the offset of that field within a record
    */
   public int offset(String fldname) {
      return offsets.get(fldname);
   }
   
   /**
    * Returns the length of a record, in bytes.
    * @return the length in bytes of a record
    */
   public int recordLength() {
      return recordlen;
   }
   
   private int lengthInBytes(String fldname) {
      int fldtype = schema.type(fldname);
      if (fldtype == INTEGER)
         return INT_SIZE;
      else
         return STR_SIZE(schema.length(fldname));
   }

    public boolean getSorted() {
        return sorted;
    }

    public void setSorted(boolean new_sorted) {
        sorted = new_sorted;
    }

    public List<String> getSortFields() {
        return sortFields;
    }

    public void setSortFields(List<String> new_sortFields) {
        sortFields = new_sortFields;
    }
}