package misc;

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class InputBufferEvent<T>
{
    public static final int DEFAULT_BUFFER_SIZE = Integer.getInteger("input_default_buffer_size", 2 * 1024);

    private MutableDirectBuffer defaultBuffer;
    private MutableDirectBuffer buffer;
    private T request;
    private long eventTime;

    public InputBufferEvent()
    {
        this(null);
    }

    public InputBufferEvent(T request)
    {
        this(request, DirectBufferAllocator.allocateDirect(DEFAULT_BUFFER_SIZE));
    }

    public InputBufferEvent(T request, ByteBuffer buffer)
    {
        this.request = request;
        this.buffer = new UnsafeBuffer(buffer);
    }

    public MutableDirectBuffer getBuffer()
    {
        return buffer;
    }

    public void growBuffer(int size)
    {
        defaultBuffer = buffer;
        buffer.wrap(DirectBufferAllocator.allocateDirect(size));
    }

    public void resetBuffer()
    {
        DirectBufferAllocator.freeDirect(buffer.byteBuffer());
        buffer = defaultBuffer;
        defaultBuffer = null;
    }

    public T getRequest()
    {
        return request;
    }

    public void setRequest(T request)
    {
        this.request = request;
    }

    public long getEventTime()
    {
        return eventTime;
    }

    public void setEventTime(long eventTime)
    {
        this.eventTime = eventTime;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("buffer=");
        builder.append(buffer);
        builder.append(", request=");
        builder.append(request != null ? request.hashCode() : null);
        builder.append(", eventTime=");
        builder.append(eventTime);

        return builder.toString();
    }
}
