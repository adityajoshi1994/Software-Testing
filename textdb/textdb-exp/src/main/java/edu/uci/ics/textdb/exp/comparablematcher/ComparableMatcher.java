package edu.uci.ics.textdb.exp.comparablematcher;

import java.text.DateFormat;
import java.util.Date;

import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.DateField;
import edu.uci.ics.textdb.api.field.DoubleField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.tuple.*;
import edu.uci.ics.textdb.exp.common.AbstractSingleInputOperator;

/**
 * ComparableMatcher is matcher for comparison query on any field which deals
 * with Comparable.
 *
 * @author Adrian Seungjin Lee
 * @author Zuozhi Wang
 */
public class ComparableMatcher extends AbstractSingleInputOperator {
    
    private ComparablePredicate predicate;
    private AttributeType inputAttrType;

    public ComparableMatcher(ComparablePredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws DataFlowException {
        outputSchema = inputOperator.getOutputSchema();
        if (! outputSchema.containsField(predicate.getAttributeName())) {
            throw new DataFlowException(String.format("attribute %s not contained in input schema %s",
                    predicate.getAttributeName(), outputSchema.getAttributeNames()));
        }
        inputAttrType = outputSchema.getAttribute(predicate.getAttributeName()).getAttributeType();
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        Tuple inputTuple;
        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            Tuple resultTuple = processOneInputTuple(inputTuple);
            if (resultTuple != null) {
                return resultTuple;
            }
        }
        return null;
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        boolean conditionSatisfied = false;
        switch (this.inputAttrType) {
        case DATE:
            conditionSatisfied = compareDate(inputTuple);
            break;
        case DOUBLE:
            conditionSatisfied = compareDouble(inputTuple);
            break;
        case INTEGER:
            conditionSatisfied = compareInt(inputTuple);
            break;
        case STRING:
        case TEXT:
        case _ID_TYPE:
            conditionSatisfied = compareString(inputTuple);
            break;
        case LIST:
            throw new DataFlowException("Unable to do comparison: LIST type is not supported");
        default:
            throw new DataFlowException("Unable to do comparison: unknown type " + inputAttrType.getName());
        }
        return conditionSatisfied ? inputTuple : null;
    }

    private boolean compareDate(Tuple inputTuple) throws DataFlowException {     
        if (predicate.getCompareToValue().getClass().equals(String.class)) {
            try {
                String compareTo = (String) predicate.getCompareToValue();
                Date compareToDate = DateFormat.getDateInstance(DateFormat.MEDIUM).parse(compareTo);
                Date date = inputTuple.getField(predicate.getAttributeName(), DateField.class).getValue();
                return compareValues(date, compareToDate, predicate.getComparisonType());
            } catch (java.text.ParseException e) {
                throw new DataFlowException("Unable to parse date: " + e.getMessage());
            }
        } else {
            throw new DataFlowException("Value " + predicate.getCompareToValue() + " is not a string");
        }
    }

    private boolean compareDouble(Tuple inputTuple) {
        Object compareToObject = predicate.getCompareToValue();
        Class<?> compareToType = compareToObject.getClass();
        Double value = inputTuple.getField(predicate.getAttributeName(), DoubleField.class).getValue();
        
        if (compareToType.equals(Integer.class)) {
            return compareValues(value, (double) (int) compareToObject, predicate.getComparisonType()); 
        } else if (compareToType.equals(Double.class)) {
            return compareValues(value, (double) compareToObject, predicate.getComparisonType());
        } else if (compareToType.equals(String.class)) {
            try {
                Double compareToValue = Double.parseDouble((String) predicate.getCompareToValue());
                return compareValues(value, compareToValue, predicate.getComparisonType());
            } catch (NumberFormatException e) {
                throw new DataFlowException("Unable to parse to number " + e.getMessage());
            }
        } else {
            throw new DataFlowException("Value " + predicate.getCompareToValue() + " is not a valid number type");
        }
    }

    private boolean compareInt(Tuple inputTuple) {
        Object compareToObject = predicate.getCompareToValue();
        Class<?> compareToType = compareToObject.getClass();
        Integer value = inputTuple.getField(predicate.getAttributeName(), IntegerField.class).getValue();
        
        if (compareToType.equals(Integer.class)) {
            return compareValues(value, (int) compareToObject, predicate.getComparisonType()); 
        } else if (compareToType.equals(Double.class)) {
            return compareValues((double) value, (double) compareToObject, predicate.getComparisonType());
        } else if (compareToType.equals(String.class)) {
            try {
                Double compareToValue = Double.parseDouble((String) predicate.getCompareToValue());
                return compareValues((double) value, compareToValue, predicate.getComparisonType());
            } catch (NumberFormatException e) {
                throw new DataFlowException("Unable to parse to number " + e.getMessage());
            }
        } else {
            throw new DataFlowException("Value " + predicate.getCompareToValue() + " is not a valid number type");
        }
    }

    private boolean compareString(Tuple inputTuple) {
        return compareValues(
                inputTuple.getField(predicate.getAttributeName(), StringField.class).getValue(),
                predicate.getCompareToValue().toString(), predicate.getComparisonType());
    }

    private static <T extends Comparable<T>> boolean compareValues(T value, T compareToValue, ComparisonType comparisonType) {
        int compareResult = value.compareTo(compareToValue);
        switch (comparisonType) {
        case EQUAL_TO:
            if (compareResult == 0) {
                return true;
            }
            break;
        case GREATER_THAN:
            if (compareResult == 1) {
                return true;
            }
            break;
        case GREATER_THAN_OR_EQUAL_TO:
            if (compareResult == 0 || compareResult == 1) {
                return true;
            }
            break;
        case LESS_THAN:
            if (compareResult == -1) {
                return true;
            }
            break;
        case LESS_THAN_OR_EQUAL_TO:
            if (compareResult == 0 || compareResult == -1) {
                return true;
            }
            break;
        case NOT_EQUAL_TO:
            if (compareResult != 0) {
                return true;
            }
            break;
        default:
            throw new DataFlowException(
                    "Unable to do comparison: unknown comparison type: " + comparisonType);
        }
        return false;
    }

    @Override
    protected void cleanUp() throws DataFlowException {
    }

}
