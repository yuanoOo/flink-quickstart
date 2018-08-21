package kudu.internal;

import org.apache.flink.types.NullFieldException;

import java.io.Serializable;

/**
 * @author zhaomingyuan
 * @date 18-8-21
 * @time 下午3:41
 */
public abstract class KuduTuple implements Serializable{
    private static final long serialVersionUID = 137578346L;

    /**
     * Gets the field at the specified position.
     *
     * @param pos The position of the field, zero indexed.
     * @return The field at the specified position.
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
     */
    public abstract <T> T getField(int pos);

    /**
     * Gets the field at the specified position, throws NullFieldException if the field is null. Used for comparing key fields.
     *
     * @param pos The position of the field, zero indexed.
     * @return The field at the specified position.
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
     * @throws NullFieldException Thrown, if the field at pos is null.
     */
    public <T> T getFieldNotNull(int pos){
        T field = getField(pos);
        if (field != null) {
            return field;
        } else {
            throw new NullFieldException(pos);
        }
    }

    /**
     * Sets the field at the specified position.
     *
     * @param value The value to be assigned to the field at the specified position.
     * @param pos The position of the field, zero indexed.
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
     */
    public abstract <T> void setField(T value, int pos);
}
