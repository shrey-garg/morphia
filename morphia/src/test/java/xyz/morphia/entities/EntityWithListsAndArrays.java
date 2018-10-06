package xyz.morphia.entities;

import org.bson.types.ObjectId;
import xyz.morphia.annotations.Id;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class EntityWithListsAndArrays {
    @Id
    private ObjectId id;
    private String[] arrayOfStrings;
    private int[] arrayOfInts;
    private short[] arrayOfShorts;
    private long[] arrayOfLongs;
    private float[] arrayOfFloats;
    private double[] arrayOfDoubles;
    private List<String> listOfStrings;
    private List<Integer> listOfIntegers;
    private List<EmbeddedType> listEmbeddedType;
    private Set<Integer> setOfIntegers;
    private String notAnArrayOrList;

    public ObjectId getId() {
        return id;
    }

    public void setId(final ObjectId id) {
        this.id = id;
    }

    public int[] getArrayOfInts() {
        return arrayOfInts;
    }

    public void setArrayOfInts(final int[] arrayOfInts) {
        this.arrayOfInts = arrayOfInts;
    }

    public String[] getArrayOfStrings() {
        return arrayOfStrings;
    }

    public void setArrayOfStrings(final String[] arrayOfStrings) {
        this.arrayOfStrings = arrayOfStrings;
    }

    public List<Integer> getListOfIntegers() {
        return listOfIntegers;
    }

    public void setListOfIntegers(final List<Integer> listOfIntegers) {
        this.listOfIntegers = listOfIntegers;
    }

    public List<String> getListOfStrings() {
        return listOfStrings;
    }

    public void setListOfStrings(final List<String> listOfStrings) {
        this.listOfStrings = listOfStrings;
    }

    public String getNotAnArrayOrList() {
        return notAnArrayOrList;
    }

    public void setNotAnArrayOrList(final String notAnArrayOrList) {
        this.notAnArrayOrList = notAnArrayOrList;
    }

    public Set<Integer> getSetOfIntegers() {
        return setOfIntegers;
    }

    public void setSetOfIntegers(final Set<Integer> setOfIntegers) {
        this.setOfIntegers = setOfIntegers;
    }

    public List<EmbeddedType> getListEmbeddedType() {
        return listEmbeddedType;
    }

    public void setListEmbeddedType(final List<EmbeddedType> listEmbeddedType) {
        this.listEmbeddedType = listEmbeddedType;
    }

    public short[] getArrayOfShorts() {
        return arrayOfShorts;
    }

    public void setArrayOfShorts(final short[] arrayOfShorts) {
        this.arrayOfShorts = arrayOfShorts;
    }

    public long[] getArrayOfLongs() {
        return arrayOfLongs;
    }

    public void setArrayOfLongs(final long[] arrayOfLongs) {
        this.arrayOfLongs = arrayOfLongs;
    }

    public float[] getArrayOfFloats() {
        return arrayOfFloats;
    }

    public void setArrayOfFloats(final float[] arrayOfFloats) {
        this.arrayOfFloats = arrayOfFloats;
    }

    public double[] getArrayOfDoubles() {
        return arrayOfDoubles;
    }

    public void setArrayOfDoubles(final double[] arrayOfDoubles) {
        this.arrayOfDoubles = arrayOfDoubles;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final EntityWithListsAndArrays entity = (EntityWithListsAndArrays) o;

        if (id != null ? !id.equals(entity.id) : entity.id != null) {
            return false;
        }
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(arrayOfStrings, entity.arrayOfStrings)) {
            return false;
        }
        if (!Arrays.equals(arrayOfInts, entity.arrayOfInts)) {
            return false;
        }
        if (!Arrays.equals(arrayOfShorts, entity.arrayOfShorts)) {
            return false;
        }
        if (!Arrays.equals(arrayOfLongs, entity.arrayOfLongs)) {
            return false;
        }
        if (!Arrays.equals(arrayOfFloats, entity.arrayOfFloats)) {
            return false;
        }
        if (!Arrays.equals(arrayOfDoubles, entity.arrayOfDoubles)) {
            return false;
        }
        if (listOfStrings != null ? !listOfStrings.equals(entity.listOfStrings) : entity.listOfStrings != null) {
            return false;
        }
        if (listOfIntegers != null ? !listOfIntegers.equals(entity.listOfIntegers) : entity.listOfIntegers != null) {
            return false;
        }
        if (listEmbeddedType != null ? !listEmbeddedType.equals(entity.listEmbeddedType) : entity.listEmbeddedType != null) {
            return false;
        }
        if (setOfIntegers != null ? !setOfIntegers.equals(entity.setOfIntegers) : entity.setOfIntegers != null) {
            return false;
        }
        return notAnArrayOrList != null ? notAnArrayOrList.equals(entity.notAnArrayOrList) : entity.notAnArrayOrList == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(arrayOfStrings);
        result = 31 * result + Arrays.hashCode(arrayOfInts);
        result = 31 * result + Arrays.hashCode(arrayOfShorts);
        result = 31 * result + Arrays.hashCode(arrayOfLongs);
        result = 31 * result + Arrays.hashCode(arrayOfFloats);
        result = 31 * result + Arrays.hashCode(arrayOfDoubles);
        result = 31 * result + (listOfStrings != null ? listOfStrings.hashCode() : 0);
        result = 31 * result + (listOfIntegers != null ? listOfIntegers.hashCode() : 0);
        result = 31 * result + (listEmbeddedType != null ? listEmbeddedType.hashCode() : 0);
        result = 31 * result + (setOfIntegers != null ? setOfIntegers.hashCode() : 0);
        result = 31 * result + (notAnArrayOrList != null ? notAnArrayOrList.hashCode() : 0);
        return result;
    }
}
