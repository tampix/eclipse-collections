package org.eclipse.collections.impl.list.immutable;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.RandomAccess;

import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.block.procedure.primitive.ObjectIntProcedure;
import org.eclipse.collections.api.list.ImmutableList;

public final class ImmutableArrayAdapter<T>
        extends AbstractImmutableList<T>
        implements Serializable, RandomAccess
{
    private static final long serialVersionUID = 1L;

    private static final ImmutableArrayAdapter<Object> EMPTY = new ImmutableArrayAdapter<>(new Object[0], 0, 0);

    private final T[] array;
    private final int from;
    private final int to;
    private final int size;

    private ImmutableArrayAdapter(T[] array)
    {
        this(array, 0, array.length - 1);
    }

    private ImmutableArrayAdapter(T[] array, int from, int to)
    {
        if (array == null)
        {
            throw new IllegalArgumentException("array cannot be null");
        }
        this.array = array;
        this.from = from;
        this.to = to;
        this.size = array.length == 0 ? 0 : Math.abs(to - from) + 1;
    }

    public static <T> ImmutableArrayAdapter<T> adapt(T[] array) {
        if (array == null)
        {
            throw new IllegalArgumentException("array cannot be null");
        }
        if (array.length == 0)
        {
            return ImmutableArrayAdapter.empty();
        }
        return new ImmutableArrayAdapter<>(array);
    }

    private static <T> ImmutableArrayAdapter<T> empty()
    {
        return (ImmutableArrayAdapter<T>) EMPTY;
    }

    private int physicalIndex(int logicalIndex)
    {
        return this.goForward()
                ? this.from + logicalIndex
                : this.from - logicalIndex;
    }

    private int logicalIndex(int physicalIndex)
    {
        return this.goForward()
                ? physicalIndex - this.from
                : this.from - physicalIndex;
    }

    /**
     * Returns true if this list is in forward order (from <= to).
     */
    private boolean goForward()
    {
        return this.from <= this.to;
    }

    @Override
    public int size()
    {
        return this.size;
    }

    @Override
    public T get(int index)
    {
        if (index < 0 || index >= this.size)
        {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + this.size);
        }
        return this.array[this.physicalIndex(index)];
    }

    @Override
    public T getFirst()
    {
        return this.isEmpty() ? null : this.array[this.from];
    }

    @Override
    public T getLast()
    {
        return this.isEmpty() ? null : this.array[this.to];
    }

    @Override
    public void each(Procedure<? super T> procedure)
    {
        if (this.goForward())
        {
            for (int i = this.from; i <= this.to; i++)
            {
                procedure.value(this.array[i]);
            }
        }
        else
        {
            for (int i = this.from; i >= this.to; i--)
            {
                procedure.value(this.array[i]);
            }
        }
    }

    @Override
    public void forEachWithIndex(ObjectIntProcedure<? super T> objectIntProcedure)
    {
        int index = 0;
        if (this.goForward())
        {
            for (int i = this.from; i <= this.to; i++)
            {
                objectIntProcedure.value(this.array[i], index++);
            }
        }
        else
        {
            for (int i = this.from; i >= this.to; i--)
            {
                objectIntProcedure.value(this.array[i], index++);
            }
        }
    }

    @Override
    public ImmutableList<T> newWith(T newItem)
    {
        int oldSize = this.size();
        T[] newArray = (T[]) new Object[oldSize + 1];
        this.toArray(newArray);
        newArray[oldSize] = newItem;
        return new ImmutableArrayAdapter<>(newArray);
    }

    @Override
    public ImmutableArrayAdapter<T> toReversed()
    {
        if (this.isEmpty())
        {
            return this;
        }
        return new ImmutableArrayAdapter<>(this.array, this.to, this.from);
    }

    @Override
    public ImmutableArrayAdapter<T> subList(int fromIndex, int toIndex)
    {
        if (fromIndex < 0)
        {
            throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);
        }
        if (toIndex > this.size)
        {
            throw new IndexOutOfBoundsException("toIndex = " + toIndex);
        }
        if (fromIndex > toIndex)
        {
            throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
        }
        if (fromIndex == toIndex)
        {
            return ImmutableArrayAdapter.empty();
        }
        if (fromIndex == 0 && toIndex == this.size)
        {
            return this;
        }

        int newFrom = this.physicalIndex(fromIndex);
        int newTo = this.physicalIndex(toIndex - 1);

        return new ImmutableArrayAdapter<>(this.array, newFrom, newTo);
    }

    @Override
    public int binarySearch(T key, Comparator<? super T> comparator)
    {
        int result;

        if (this.goForward())
        {
            result = Arrays.binarySearch(this.array, this.from, this.to + 1, key, comparator);
        }
        else
        {
            Comparator<? super T> reversedComparator = Collections.reverseOrder(comparator);
            result = Arrays.binarySearch(this.array, this.to, this.from + 1, key, reversedComparator);
        }

        if (result >= 0)
        {
            return this.logicalIndex(result);
        }
        else
        {
            // result is -(insertion point) - 1
            int insertionPoint = -(result + 1);
            return -(this.logicalIndex(insertionPoint) + 1);
        }
    }

    private Object writeReplace()
    {
        if (this.isEmpty())
        {
            return this;
        }
        if (this.from == 0 && this.to == this.array.length - 1)
        {
            return this;
        }
        T[] normalized = (T[]) this.toArray();
        return new ImmutableArrayAdapter<>(normalized);
    }

    @Override
    public Object[] toArray()
    {
        if (!this.goForward())
        {
            return super.toArray();
        }
        return Arrays.copyOfRange(this.array, this.from, this.to + 1);
    }

    @Override
    public <E> E[] toArray(E[] a)
    {
        if (!this.goForward())
        {
            return super.toArray(a);
        }
        E[] result = a.length >= this.size
                ? a
                : (E[]) Array.newInstance(a.getClass().getComponentType(), this.size);

        System.arraycopy(this.array, this.from, result, 0, this.size);
        if (result.length > this.size)
        {
            result[this.size] = null;
        }
        return result;
    }

}
