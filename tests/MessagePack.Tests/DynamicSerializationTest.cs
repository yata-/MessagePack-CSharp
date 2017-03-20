using MessagePack.Resolvers;
using SharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace MessagePack.Tests
{
    public class DynamicSerializationTest
    {
        [Fact]
        public void SerializeDynamic()
        {
            var resolver = StandardResolver.Instance;

            {
                byte[] bytes = null;
                var v = resolver.SerializeDynamic(typeof(int), ref bytes, 0, 1000);

                int readSize;
                var vv = resolver.DeserializeDynamic(typeof(int), bytes, 0, out readSize);

                vv.Is(1000);
            }
            {
                byte[] bytes = null;
                var v = resolver.SerializeDynamic(typeof(int), ref bytes, 0, 1000);

                int readSize;
                var vv = resolver.DeserializeDynamic(typeof(int), bytes, 0, out readSize);

                ((int)vv).Is(1000);
            }

            var data = new SharedData.FirstSimpleData()
            {
                Prop1 = 10,
                Prop2 = "aaa",
                Prop3 = 1000
            };

            {
                byte[] bytes = null;
                var v = resolver.SerializeDynamic(typeof(FirstSimpleData), ref bytes, 0, data);

                int readSize;
                var vv = resolver.DeserializeDynamic(typeof(FirstSimpleData), bytes, 0, out readSize);

                ((FirstSimpleData)vv).IsStructuralEqual(data);
            }
            {
                byte[] bytes = null;
                var v = resolver.SerializeDynamic(typeof(FirstSimpleData), ref bytes, 0, data);

                int readSize;
                var vv = resolver.DeserializeDynamic(typeof(FirstSimpleData), bytes, 0, out readSize);

                ((FirstSimpleData)vv).IsStructuralEqual(data);
            }

            {
                byte[] bytes = null;
                var v = resolver.SerializeDynamic(typeof(IntEnum), ref bytes, 0, IntEnum.D);

                int readSize;
                var vv = resolver.DeserializeDynamic(typeof(IntEnum), bytes, 0, out readSize);

                ((IntEnum)vv).Is(IntEnum.D);
            }
            {
                byte[] bytes = null;
                var v = resolver.SerializeDynamic(typeof(IntEnum), ref bytes, 0, IntEnum.D);

                int readSize;
                var vv = resolver.DeserializeDynamic(typeof(IntEnum), bytes, 0, out readSize);

                ((IntEnum)vv).Is(IntEnum.D);
            }
        }

        T EmbeddedTypeConvert<T>(T value)
        {
            return MessagePackSerializer.Deserialize<T>(MessagePackSerializer.Serialize(value, MessagePack.Resolvers.EmbeddedTypeResolver.Instance), MessagePack.Resolvers.EmbeddedTypeResolver.Instance);
        }

        public static object testData = new object[]
        {
            new object[]{ 100,100, null },
            new object[]{ 123.456,123.456, null },
            new object[]{ IntEnum.C, IntEnum.C, null },
        };

        [Theory]
        [MemberData(nameof(testData))]
        public void EmbeddedFormatter<T>(T v, T? v1, T? v2)
            where T : struct
        {
            EmbeddedTypeConvert(v).Is(v);
            EmbeddedTypeConvert(v1).Is(v1);
            EmbeddedTypeConvert(v2).Is(v2);
        }


    }
}
