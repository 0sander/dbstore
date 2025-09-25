package com.cinefms.dbstore.api;

/**
 * Represents a field update operation with its type and value
 */
public class FieldUpdate {
    private final String fieldName;
    private final UpdateOperation operation;
    private final Object value;

    public FieldUpdate(String fieldName, UpdateOperation operation, Object value) {
        this.fieldName = fieldName;
        this.operation = operation;
        this.value = value;
    }

    public String getFieldName() {
        return fieldName;
    }

    public UpdateOperation getOperation() {
        return operation;
    }

    public Object getValue() {
        return value;
    }

    // Convenience static methods for common operations
    public static FieldUpdate set(String fieldName, Object value) {
        return new FieldUpdate(fieldName, UpdateOperation.SET, value);
    }

    public static FieldUpdate inc(String fieldName, Number increment) {
        return new FieldUpdate(fieldName, UpdateOperation.INC, increment);
    }

    public static FieldUpdate unset(String fieldName) {
        return new FieldUpdate(fieldName, UpdateOperation.UNSET, null);
    }

    public static FieldUpdate push(String fieldName, Object value) {
        return new FieldUpdate(fieldName, UpdateOperation.PUSH, value);
    }

    public static FieldUpdate pull(String fieldName, Object value) {
        return new FieldUpdate(fieldName, UpdateOperation.PULL, value);
    }

    public static FieldUpdate addToSet(String fieldName, Object value) {
        return new FieldUpdate(fieldName, UpdateOperation.ADD_TO_SET, value);
    }

    public static FieldUpdate mul(String fieldName, Number multiplier) {
        return new FieldUpdate(fieldName, UpdateOperation.MUL, multiplier);
    }

    public static FieldUpdate min(String fieldName, Object value) {
        return new FieldUpdate(fieldName, UpdateOperation.MIN, value);
    }

    public static FieldUpdate max(String fieldName, Object value) {
        return new FieldUpdate(fieldName, UpdateOperation.MAX, value);
    }

    public static FieldUpdate rename(String fieldName, String newFieldName) {
        return new FieldUpdate(fieldName, UpdateOperation.RENAME, newFieldName);
    }

    public static FieldUpdate setOnInsert(String fieldName, Object value) {
        return new FieldUpdate(fieldName, UpdateOperation.SET_ON_INSERT, value);
    }
}
