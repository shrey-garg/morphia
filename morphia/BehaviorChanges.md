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
1. Locale support no longer native.  I'm not sure it makes sense to have all these one off conversions in Morphia itself given how simple
 it is to write and register custom codecs.
1. Top level generic types are no longer mappable.  This is a shortcoming with the driver mapping code and may or may not be addressed in
 the future.
1. Entity.noClassnameStored has been removed noClassNameStored() because it was awkwardly named and I can't determine between a legacy use of that and a modern use of useDiscriminator
1. throw an exception when getting the collection for an unmapped type
1. removed support for @Property.concreteClass().  It was meant to be able to specify the actual interface implementation backing a 
property.  The current driver infrastructure makes that difficult to recreate.  I'm unsure how used this feature ever was so I'm removing
 it for now.  We can examine how to resurrect should the need arise. 
1. @ConstructorArgs has been removed since the driver code supports constructor based instantiation.
1. Migrated uses of `CodeWScope` to `CodeWithScope`
1. `Mapper` is no longer independent from `Datastore`.  To get started, one calls one of the factory methods on `Morphia` to create a new
 `Datastore` and the `Mapper` is retrieved from that.