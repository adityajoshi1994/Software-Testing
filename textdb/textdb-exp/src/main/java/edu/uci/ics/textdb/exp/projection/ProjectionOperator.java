package edu.uci.ics.textdb.exp.projection;

import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.common.AbstractSingleInputOperator;

public class ProjectionOperator extends AbstractSingleInputOperator {
    
    ProjectionPredicate predicate;
    
    Schema inputSchema;
    
    public ProjectionOperator(ProjectionPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws TextDBException {
        inputSchema = inputOperator.getOutputSchema();
        List<Attribute> outputAttributes = 
                inputSchema.getAttributes()
                .stream()
                .filter(attr -> predicate.getProjectionFields().contains(attr.getAttributeName().toLowerCase()))
                .collect(Collectors.toList());
        
        if (outputAttributes.size() != predicate.getProjectionFields().size()) {
            throw new DataFlowException("input schema doesn't contain one of the attributes to be projected");
        }
        outputSchema = new Schema(outputAttributes.stream().toArray(Attribute[]::new));
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        Tuple inputTuple = inputOperator.getNextTuple();
        if (inputTuple == null) {
            return null;
        }

        return processOneInputTuple(inputTuple);
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        IField[] outputFields =
                outputSchema.getAttributes()
                        .stream()
                        .map(attr -> inputTuple.getField(attr.getAttributeName()))
                        .toArray(IField[]::new);

        return new Tuple(outputSchema, outputFields);
    }

    @Override
    protected void cleanUp() throws DataFlowException {        
    }

    public ProjectionPredicate getPredicate() {
        return predicate;
    }
}
