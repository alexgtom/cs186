package simpledb;

import java.lang.*;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */

public class IntegerAggregator implements Aggregator {
	
    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;

    private HashMap<Field, ArrayList<Field>> groups;
    private ArrayList<Field> nogroup;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;

        if(gbfield == Aggregator.NO_GROUPING)
            nogroup = new ArrayList<Field>();
        else
            this.groups = new HashMap<Field, ArrayList<Field>>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(this.gbfield != Aggregator.NO_GROUPING) {
            Field group = tup.getField(this.gbfield);
            Field value = tup.getField(this.afield);

            ArrayList<Field> values = groups.get(group);
            if(values == null)
                values = new ArrayList<Field>();

            values.add(value);
            groups.put(group,values);
        } else {
            nogroup.add(tup.getField(this.afield));
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc td;
        if(this.gbfield != Aggregator.NO_GROUPING){
            td = new TupleDesc(new Type[]{ gbfieldtype, Type.INT_TYPE});

            for(Field f : groups.keySet()) {
                ArrayList<Field> values = groups.get(f);
                Tuple t = new Tuple(td);
                t.setField(0, f);
                t.setField(1, new IntField(aggregate(values)));
                tuples.add(t);
            }
        } else {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(aggregate(nogroup)));

            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

    private int aggregate(ArrayList<Field> values){
        if(this.op == Op.COUNT)
            return count(values);
        else if(this.op == Op.SUM)
            return sum(values);
        else if(this.op == Op.AVG)
            return avg(values);
        else if(this.op == Op.MAX)
            return max(values);
        else 
            return min(values);
    }

    private int count(ArrayList<Field> values) {
        return values.size();
    }

    private int sum(ArrayList<Field>values) {
        int sum = 0;
        for(Field f : values)
            sum += ((IntField) f).getValue();
        return sum;
    }

    private int avg(ArrayList<Field>values) {
        return sum(values) / count(values);
    }

    private int max(ArrayList<Field>values) {
        int max = Integer.MIN_VALUE;
        for(Field f : values) {
            int v = ((IntField) f).getValue();
            if (v > max)
                max = v;
        }
        return max;
    }

    private int min(ArrayList<Field>values) {
        int min = Integer.MAX_VALUE;
        for(Field f : values) {
            int v = ((IntField) f).getValue();
            if (v < min)
                min = v;
        }
        return min;
    }
}
