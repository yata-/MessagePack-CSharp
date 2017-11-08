using MessagePack.Internal;
using System;
using System.IO;
using MessagePack.LZ4;
using System.Buffers;

namespace MessagePack
{
    /// <summary>
    /// LZ4 Compressed special serializer.
    /// </summary>
    public static partial class LZ4MessagePackSerializer
    {
        public const sbyte ExtensionTypeCode = 99;

        public const int NotCompressionSize = 64;

        const int ThreadStaticBufferSize = 65535;

        [ThreadStatic]
        static byte[] ThreadStaticBuffer;

        /// <summary>
        /// Serialize to binary with default resolver.
        /// </summary>
        public static byte[] Serialize<T>(T obj)
        {
            return Serialize(obj, null);
        }

        /// <summary>
        /// Serialize to binary with specified resolver.
        /// </summary>
        public static byte[] Serialize<T>(T obj, IFormatterResolver resolver)
        {
            return Serialize<T>(obj, resolver, MessagePackSerializer.DefaultBufferPool, MessagePackSerializer.DefaultBufferPoolMinimumLength);
        }

        /// <summary>
        /// Serialize to binary with specified resolver and buffer pool.
        /// </summary>
        public static byte[] Serialize<T>(T obj, IFormatterResolver resolver, ArrayPool<byte> pool, int minimumLength)
        {
            if (resolver == null) resolver = MessagePackSerializer.DefaultResolver;

            var rentBuffer1 = pool.Rent(minimumLength);
            var rentBuffer2 = pool.Rent(minimumLength);
            try
            {
                var buffer = SerializeCore(obj, resolver, rentBuffer1, rentBuffer2);
                return MessagePackBinary.FastCloneWithResize(buffer.Array, buffer.Count);
            }
            finally
            {
                pool.Return(rentBuffer1, MessagePackSerializer.IsClearBufferWhenReturning);
                pool.Return(rentBuffer2, MessagePackSerializer.IsClearBufferWhenReturning);
            }
        }

        /// <summary>
        /// Serialize to stream.
        /// </summary>
        public static void Serialize<T>(Stream stream, T obj)
        {
            Serialize(stream, obj, null);
        }

        /// <summary>
        /// Serialize to stream with specified resolver.
        /// </summary>
        public static void Serialize<T>(Stream stream, T obj, IFormatterResolver resolver)
        {
            Serialize<T>(stream, obj, resolver, MessagePackSerializer.DefaultBufferPool, MessagePackSerializer.DefaultBufferPoolMinimumLength);
        }

        /// <summary>
        /// Serialize to stream with specified resolver.
        /// </summary>
        public static void Serialize<T>(Stream stream, T obj, IFormatterResolver resolver, ArrayPool<byte> pool, int minimumLength)
        {
            if (resolver == null) resolver = MessagePackSerializer.DefaultResolver;

            var rentBuffer1 = pool.Rent(minimumLength);
            var rentBuffer2 = pool.Rent(minimumLength);
            try
            {
                var buffer = SerializeCore(obj, resolver, rentBuffer1, rentBuffer2);
                stream.Write(buffer.Array, 0, buffer.Count);
            }
            finally
            {
                pool.Return(rentBuffer1, MessagePackSerializer.IsClearBufferWhenReturning);
                pool.Return(rentBuffer2, MessagePackSerializer.IsClearBufferWhenReturning);
            }
        }

        public static int SerializeToBlock<T>(ref byte[] bytes, int offset, T obj, IFormatterResolver resolver)
        {
            return SerializeToBlock<T>(ref bytes, offset, obj, resolver, MessagePackSerializer.DefaultBufferPool, MessagePackSerializer.DefaultBufferPoolMinimumLength);
        }

        public static int SerializeToBlock<T>(ref byte[] bytes, int offset, T obj, IFormatterResolver resolver, ArrayPool<byte> pool, int minimumLength)
        {
            var originalBuffer = pool.Rent(minimumLength);
            try
            {
                var serializedData = MessagePackSerializer.SerializeUnsafe(obj, resolver, originalBuffer);

                if (serializedData.Count < NotCompressionSize)
                {
                    // can't write direct, shoganai...
                    MessagePackBinary.EnsureCapacity(ref bytes, offset, serializedData.Count);
                    Buffer.BlockCopy(serializedData.Array, serializedData.Offset, bytes, offset, serializedData.Count);
                    return serializedData.Count;
                }
                else
                {
                    var maxOutCount = LZ4Codec.MaximumOutputLength(serializedData.Count);

                    MessagePackBinary.EnsureCapacity(ref bytes, offset, 6 + 5 + maxOutCount); // (ext header size + fixed length size)

                    // acquire ext header position
                    var extHeaderOffset = offset;
                    offset += (6 + 5);

                    // write body
                    var lz4Length = LZ4Codec.Encode(serializedData.Array, serializedData.Offset, serializedData.Count, bytes, offset, bytes.Length - offset);

                    // write extension header(always 6 bytes)
                    extHeaderOffset += MessagePackBinary.WriteExtensionFormatHeaderForceExt32Block(ref bytes, extHeaderOffset, (sbyte)ExtensionTypeCode, lz4Length + 5);

                    // write length(always 5 bytes)
                    MessagePackBinary.WriteInt32ForceInt32Block(ref bytes, extHeaderOffset, serializedData.Count);

                    return 6 + 5 + lz4Length;
                }
            }
            finally
            {
                pool.Return(originalBuffer, MessagePackSerializer.IsClearBufferWhenReturning);
            }
        }

        public static byte[] ToLZ4Binary(ArraySegment<byte> messagePackBinary)
        {
            return ToLZ4Binary(messagePackBinary, MessagePackSerializer.DefaultBufferPool, MessagePackSerializer.DefaultBufferPoolMinimumLength);
        }

        public static byte[] ToLZ4Binary(ArraySegment<byte> messagePackBinary, ArrayPool<byte> pool, int minimumLength)
        {
            var rentBuffer = pool.Rent(minimumLength);
            try
            {
                var buffer = ToLZ4BinaryCore(messagePackBinary, rentBuffer);
                return MessagePackBinary.FastCloneWithResize(buffer.Array, buffer.Count);
            }
            finally
            {
                pool.Return(rentBuffer, MessagePackSerializer.IsClearBufferWhenReturning);
            }
        }

        static ArraySegment<byte> SerializeCore<T>(T obj, IFormatterResolver resolver, byte[] serializeInitialBuffer, byte[] lz4InitialBuffer)
        {
            var serializedData = MessagePackSerializer.SerializeUnsafe(obj, resolver, serializeInitialBuffer);
            return ToLZ4BinaryCore(serializedData, lz4InitialBuffer);
        }

        static ArraySegment<byte> ToLZ4BinaryCore(ArraySegment<byte> serializedData, byte[] lz4InitialBuffer)
        {
            if (serializedData.Count < NotCompressionSize)
            {
                return serializedData;
            }
            else
            {
                var offset = 0;
                var buffer = lz4InitialBuffer;
                var maxOutCount = LZ4Codec.MaximumOutputLength(serializedData.Count);
                if (buffer.Length + 6 + 5 < maxOutCount) // (ext header size + fixed length size)
                {
                    buffer = new byte[6 + 5 + maxOutCount];
                }

                // acquire ext header position
                var extHeaderOffset = offset;
                offset += (6 + 5);

                // write body
                var lz4Length = LZ4Codec.Encode(serializedData.Array, serializedData.Offset, serializedData.Count, buffer, offset, buffer.Length - offset);

                // write extension header(always 6 bytes)
                extHeaderOffset += MessagePackBinary.WriteExtensionFormatHeaderForceExt32Block(ref buffer, extHeaderOffset, (sbyte)ExtensionTypeCode, lz4Length + 5);

                // write length(always 5 bytes)
                MessagePackBinary.WriteInt32ForceInt32Block(ref buffer, extHeaderOffset, serializedData.Count);

                return new ArraySegment<byte>(buffer, 0, 6 + 5 + lz4Length);
            }
        }

        public static T Deserialize<T>(byte[] bytes)
        {
            return Deserialize<T>(bytes, null);
        }

        public static T Deserialize<T>(byte[] bytes, IFormatterResolver resolver)
        {
            return DeserializeCore<T>(new ArraySegment<byte>(bytes, 0, bytes.Length), resolver, MessagePackSerializer.DefaultBufferPool);
        }

        public static T Deserialize<T>(byte[] bytes, IFormatterResolver resolver, ArrayPool<byte> pool)
        {
            return DeserializeCore<T>(new ArraySegment<byte>(bytes, 0, bytes.Length), resolver, pool);
        }

        public static T Deserialize<T>(ArraySegment<byte> bytes)
        {
            return DeserializeCore<T>(bytes, null, MessagePackSerializer.DefaultBufferPool);
        }

        public static T Deserialize<T>(ArraySegment<byte> bytes, IFormatterResolver resolver)
        {
            return DeserializeCore<T>(bytes, resolver, MessagePackSerializer.DefaultBufferPool);
        }

        public static T Deserialize<T>(ArraySegment<byte> bytes, IFormatterResolver resolver, ArrayPool<byte> pool)
        {
            return DeserializeCore<T>(bytes, resolver, pool);
        }

        public static T Deserialize<T>(Stream stream)
        {
            return Deserialize<T>(stream, null);
        }

        public static T Deserialize<T>(Stream stream, IFormatterResolver resolver)
        {
            return Deserialize<T>(stream, resolver, false);
        }

        public static T Deserialize<T>(Stream stream, bool readStrict)
        {
            return Deserialize<T>(stream, MessagePackSerializer.DefaultResolver, readStrict);
        }

        public static T Deserialize<T>(Stream stream, IFormatterResolver resolver, bool readStrict)
        {
            return Deserialize<T>(stream, resolver, readStrict, MessagePackSerializer.DefaultBufferPool, MessagePackSerializer.DefaultBufferPoolMinimumLength);
        }

        public static T Deserialize<T>(Stream stream, IFormatterResolver resolver, bool readStrict, ArrayPool<byte> pool, int minimumLength)
        {
            if (!readStrict)
            {
                var rentBuffer = pool.Rent(minimumLength);
                var buffer = rentBuffer;
                try
                {
                    var len = FillFromStream(stream, ref buffer);
                    return DeserializeCore<T>(new ArraySegment<byte>(buffer, 0, len), resolver, pool);
                }
                finally
                {
                    pool.Return(rentBuffer, MessagePackSerializer.IsClearBufferWhenReturning);
                }
            }
            else
            {
                int blockSize;
                var bytes = MessagePackBinary.ReadMessageBlockFromStreamUnsafe(stream, false, out blockSize);
                return DeserializeCore<T>(new ArraySegment<byte>(bytes, 0, blockSize), resolver, pool);
            }
        }

        public static byte[] Decode(Stream stream, bool readStrict = false)
        {
            return Decode(stream, readStrict, MessagePackSerializer.DefaultBufferPool, MessagePackSerializer.DefaultBufferPoolMinimumLength);
        }

        public static byte[] Decode(Stream stream, bool readStrict, ArrayPool<byte> pool, int minimumLength)
        {
            if (!readStrict)
            {
                var rentBuffer = pool.Rent(minimumLength);
                var buffer = rentBuffer;
                try
                {
                    var len = FillFromStream(stream, ref buffer);
                    return Decode(new ArraySegment<byte>(buffer, 0, len));
                }
                finally
                {
                    pool.Return(rentBuffer, MessagePackSerializer.IsClearBufferWhenReturning);
                }
            }
            else
            {
                int blockSize;
                var bytes = MessagePackBinary.ReadMessageBlockFromStreamUnsafe(stream, false, out blockSize);
                return Decode(new ArraySegment<byte>(bytes, 0, blockSize));
            }
        }

        public static byte[] Decode(byte[] bytes)
        {
            return Decode(new ArraySegment<byte>(bytes, 0, bytes.Length));
        }

        public static byte[] Decode(ArraySegment<byte> bytes)
        {
            int readSize;
            if (MessagePackBinary.GetMessagePackType(bytes.Array, bytes.Offset) == MessagePackType.Extension)
            {
                var header = MessagePackBinary.ReadExtensionFormatHeader(bytes.Array, bytes.Offset, out readSize);
                if (header.TypeCode == ExtensionTypeCode)
                {
                    // decode lz4
                    var offset = bytes.Offset + readSize;
                    var length = MessagePackBinary.ReadInt32(bytes.Array, offset, out readSize);
                    offset += readSize;

                    var buffer = new byte[length]; // use new buffer.

                    // LZ4 Decode
                    var len = bytes.Count + bytes.Offset - offset;
                    LZ4Codec.Decode(bytes.Array, offset, len, buffer, 0, length);

                    return buffer;
                }
            }

            if (bytes.Offset == 0 && bytes.Array.Length == bytes.Count)
            {
                // return same reference
                return bytes.Array;
            }
            else
            {
                var result = new byte[bytes.Count];
                Buffer.BlockCopy(bytes.Array, bytes.Offset, result, 0, result.Length);
                return result;
            }
        }

        static T DeserializeCore<T>(ArraySegment<byte> bytes, IFormatterResolver resolver, ArrayPool<byte> pool)
        {
            if (resolver == null) resolver = MessagePackSerializer.DefaultResolver;
            var formatter = resolver.GetFormatterWithVerify<T>();

            int readSize;
            if (MessagePackBinary.GetMessagePackType(bytes.Array, bytes.Offset) == MessagePackType.Extension)
            {
                var header = MessagePackBinary.ReadExtensionFormatHeader(bytes.Array, bytes.Offset, out readSize);
                if (header.TypeCode == ExtensionTypeCode)
                {
                    // decode lz4
                    var offset = bytes.Offset + readSize;
                    var length = MessagePackBinary.ReadInt32(bytes.Array, offset, out readSize);
                    offset += readSize;

                    var rentBuffer = pool.Rent(length);
                    try
                    {
                        var buffer = rentBuffer;
                        // LZ4 Decode
                        var len = bytes.Count + bytes.Offset - offset;
                        LZ4Codec.Decode(bytes.Array, offset, len, buffer, 0, length);

                        return formatter.Deserialize(buffer, 0, resolver, out readSize);
                    }
                    finally
                    {
                        pool.Return(rentBuffer, MessagePackSerializer.IsClearBufferWhenReturning);
                    }
                }
            }

            return formatter.Deserialize(bytes.Array, bytes.Offset, resolver, out readSize);
        }

        static int FillFromStream(Stream input, ref byte[] buffer)
        {
            int length = 0;
            int read;
            while ((read = input.Read(buffer, length, buffer.Length - length)) > 0)
            {
                length += read;
                if (length == buffer.Length)
                {
                    MessagePackBinary.FastResize(ref buffer, length * 2);
                }
            }

            return length;
        }
    }
}