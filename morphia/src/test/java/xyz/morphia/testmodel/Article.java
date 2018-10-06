package xyz.morphia.testmodel;


import xyz.morphia.annotations.Entity;
import xyz.morphia.annotations.Property;
import xyz.morphia.annotations.Reference;
import xyz.morphia.testutil.TestEntity;

import java.util.HashMap;
import java.util.Map;

@Entity("articles")
@SuppressWarnings("unchecked")
public class Article extends TestEntity {
    private Map<String, Translation> translations;
    @Property
    private Map attributes;
    @Reference
    private Map<String, Article> related;

    public Article() {
        translations = new HashMap<>();
        attributes = new HashMap<String, Object>();
        related = new HashMap<>();
    }

    public Object getAttribute(final String name) {
        return attributes.get(name);
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(final Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public Map<String, Article> getRelated() {
        return related;
    }

    public void setRelated(final Map<String, Article> related) {
        this.related = related;
    }

    public Article getRelated(final String name) {
        return related.get(name);
    }

    public Translation getTranslation(final String langCode) {
        return translations.get(langCode);
    }

    public Map<String, Translation> getTranslations() {
        return translations;
    }

    public void setTranslations(final Map<String, Translation> translations) {
        this.translations = translations;
    }

    public void putRelated(final String name, final Article a) {
        related.put(name, a);
    }

    public void setAttribute(final String name, final Object value) {
        attributes.put(name, value);
    }

    public void setTranslation(final String langCode, final Translation t) {
        translations.put(langCode, t);
    }
}
