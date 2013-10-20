package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        child1tuple = null;
        child2tuple = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    private Tuple child1tuple = null;
    private Tuple child2tuple = null;
    private boolean stop = false;

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (stop)
            return null;
        if (child1.hasNext() && child1tuple == null) {
            child1tuple = child1.next();
        }
        if (child2.hasNext() && child2tuple == null) {
            child2tuple = child2.next();
        }

        while(true) {
            Tuple t = null;
            
            if (p.filter(child1tuple, child2tuple))
                t = mergeTuples(child1tuple, child2tuple);

            if (!child2.hasNext()) {
                if (!child1.hasNext()) {
                    // !child1.hasNext() and !child2.hasNext()
                    if (t != null) {
                        stop = true;
                        return t;
                    } else {
                        return null;
                    }
                } else {
                    child1tuple = child1.next();
                }
                child2.rewind();
            }
            child2tuple = child2.next();

            if (t != null) {
                return t;
            }
        }
    }

    private Tuple mergeTuples(Tuple t1, Tuple t2) {
        // we found a valid match, now merge the tuples together
        Tuple t = new Tuple(getTupleDesc());
        int fieldCount = 0;
        for(Iterator<Field> fields = t1.fields(); fields.hasNext(); fieldCount++) {
            t.setField(fieldCount, fields.next());
        }
        for(Iterator<Field> fields = t2.fields(); fields.hasNext(); fieldCount++) {
            t.setField(fieldCount, fields.next());
        }
        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {child1, child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if(children.length == 2) {
            child1 = children[0];
            child1 = children[1];
        }
    }

}
