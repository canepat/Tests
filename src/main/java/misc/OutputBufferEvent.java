package misc;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class OutputBufferEvent<T>
{
    public static final int DEFAULT_BUFFER_SIZE = Integer.getInteger("output_default_buffer_size", 2 * 1024);

    private MutableDirectBuffer defaultBuffer;
    private MutableDirectBuffer buffer;
    private T response;
    private long eventTime;

    public OutputBufferEvent()
    {
        this(null);
    }

    public OutputBufferEvent(T response)
    {
        this(response, DirectBufferAllocator.allocateDirect(DEFAULT_BUFFER_SIZE));
    }

    public OutputBufferEvent(T response, ByteBuffer buffer)
    {
        this.response = response;
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

    public T getResponse()
    {
        return response;
    }

    public void setResponse(T response)
    {
        this.response = response;
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
        builder.append(", response=");
        builder.append(response);
        builder.append(", eventTime=");
        builder.append(eventTime);

        return builder.toString();
    }
}
