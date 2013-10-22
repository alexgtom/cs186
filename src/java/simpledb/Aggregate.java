package simpledb;

import java.util.*;
import java.lang.*;
import java.io.IOException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private DbIterator aggregator;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        if(gfield == -1)
            return Aggregator.NO_GROUPING;
        else
            return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    	if (gfield == Aggregator.NO_GROUPING)
    		return null;
    	else
            return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }
    
    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        child.open();
        super.open();

        Type gtype = null;
        if (groupField() != Aggregator.NO_GROUPING)
            gtype = child.getTupleDesc().getFieldType(gfield);

        Aggregator ag;
	    if (child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
	    	ag = new IntegerAggregator(gfield, gtype, afield, aop);
	    } else {
	    	ag = new StringAggregator(gfield, gtype, afield, aop);
	    }

        while (child.hasNext())
            ag.mergeTupleIntoGroup(child.next());
        child.close();

        aggregator = ag.iterator();
        aggregator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    if (aggregator.hasNext())
	    	return aggregator.next();
        else
            return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    aggregator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    TupleDesc td = child.getTupleDesc();
	    String aname = td.getFieldName(afield);
	    Type atype = td.getFieldType(afield);

	    if (groupField() != Aggregator.NO_GROUPING) {
	    	Type type = td.getFieldType(gfield);
	    	String name = td.getFieldName(gfield);
	    	return new TupleDesc(new Type[]{ atype, type }, new String[]{ aname, name});
        } else {
	    	return new TupleDesc(new Type[]{ atype }, new String[]{ aname });
	    }
    }

    public void close() {
        super.close();
	    aggregator.close();
    }

    @Override
    public DbIterator[] getChildren() {
	    return new DbIterator[] {aggregator};
    }

    @Override
    public void setChildren(DbIterator[] children) {
	    child = children[0];
    }
    
}
