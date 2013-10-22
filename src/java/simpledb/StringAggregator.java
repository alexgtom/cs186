package simpledb;

import java.util.*;
import java.lang.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;

    private HashMap<Field, ArrayList<Field>> groups;
    private ArrayList<Field> nogroup;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
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
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc td;

        if (this.gbfield != Aggregator.NO_GROUPING){
            td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });

            for(Field f : groups.keySet()) {
                Tuple t = new Tuple(td);
                t.setField(0, f);
                t.setField(1, new IntField(groups.get(f).size()));
                tuples.add(t);
            }
        } else {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(nogroup.size()));

            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

}
