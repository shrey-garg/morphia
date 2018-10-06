package xyz.morphia.query;


import org.bson.Document;


/**
 * Internal class for building up query documents.
 */
public interface Criteria {
    /**
     * Adds this Criteria's information to the Document
     *
     * @param obj the Document to update
     */
    void addTo(Document obj);

    /**
     * Used to add this Criteria to a CriteriaContainer
     *
     * @param container the container to add to
     */
    void attach(CriteriaContainer container);

    /**
     * @return the field name for the criteria
     */
    String getFieldName();
}
