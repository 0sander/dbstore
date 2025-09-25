package com.cinefms.dbstore.api;

/**
 * Represents different types of MongoDB update operations
 */
public enum UpdateOperation {
    SET,    // $set - set field value
    INC,    // $inc - increment numeric field
    UNSET,  // $unset - remove field
    PUSH,   // $push - add to array
    PULL,   // $pull - remove from array
    ADD_TO_SET, // $addToSet - add to array if not exists
    MUL,    // $mul - multiply numeric field
    MIN,    // $min - set to minimum value
    MAX,    // $max - set to maximum value
    RENAME, // $rename - rename field
    SET_ON_INSERT // $setOnInsert - set only on insert
}
