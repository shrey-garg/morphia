package org.mongodb.morphia.query;


import org.bson.Document;
import org.bson.types.CodeWithScope;

/**
 * Creates a Criteria for a $where clause.
 */
public class WhereCriteria extends AbstractCriteria {

    private final Object js;

    /**
     * Creates a WhereCriteria with the given javascript
     *
     * @param js the javascript
     */
    public WhereCriteria(final String js) {
        this.js = js;
    }

    /**
     * Creates a WhereCriteria with the given javascript
     *
     * @param js the javascript
     */
    public WhereCriteria(final CodeWithScope js) {
        this.js = js;
    }

    @Override
    public void addTo(final Document obj) {
        obj.put(FilterOperator.WHERE.val(), js);
    }

    @Override
    public String getFieldName() {
        return FilterOperator.WHERE.val();
    }

}
