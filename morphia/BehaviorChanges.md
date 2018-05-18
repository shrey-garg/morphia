In this document, I'm going to try and document every deviation from 1.3 that I can.  My hope is that I can find a mitigation to any such
 deviation but, barring that, at least document it so that end users are not caught off guard.  If an undocumented change is found, 
 please file and issue or a pull request to this file with the details so I can explore options.
 
### Changes
1. Duplicate properties now result in a `CodecConfigurationException` instead of a `ConstraintViolationException`.  Both are runtime 
exceptions and almost certainly not caught by user code.
1. Instances of `com.mongodb.DBObject` have been replaced `org.bson.Document`.  I'm exploring inserting a shim interface in the `org
.mongodb.morphia.Datastore` hierarchy to ease this transition but I'll have to see how dramatic a change that is once everything is done.
1. The geo API is going to be heavily reworked to use driver primitives rather than Morphia wrappers.
1. Duplicate IDs result in a CodecConfigurationException rather than a ConstraintViolationException.  The checks are happening in the 
driver before Morphia gets a chance to do any validation.  Rather than engineering complex solutions to maintain the current behavior, 
I'm just updating tests to check for the new Exception type instead.  These are runtime exceptions that few, if any, will be explicitly 
catching so this change should be source compatible for almost everyone. 
    * Some others validations may get caught up in this change as well.  If I've missed documenting them, please file an issue.
1. `@PreSave` has been removed in favor of `@PrePersist`.
1. Return values of lifecycle events are no longer considered.  Any evolutions of the `Document` should be done in place.