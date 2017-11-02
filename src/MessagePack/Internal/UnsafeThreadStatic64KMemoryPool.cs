using System;
using System.Buffers;

namespace MessagePack.Internal
{
    internal class UnsafeThreadStatic64KMemoryPool : ArrayPool<byte>
    {
        public const int BufferSize = 65535;
        public static readonly UnsafeThreadStatic64KMemoryPool Instance = new UnsafeThreadStatic64KMemoryPool();

        UnsafeThreadStatic64KMemoryPool()
        {

        }

        [ThreadStatic]
        static byte[] buffer;

        public byte[] GetBuffer()
        {
            return Rent(BufferSize);
        }

        public override byte[] Rent(int minimumLength)
        {
            // ignore minimum length
            if (buffer == null)
            {
                buffer = new byte[BufferSize];
            }
            return buffer;
        }

        public override void Return(byte[] array, bool clearArray = false)
        {
            // ignore
        }
    }
}