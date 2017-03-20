using MessagePack.Formatters;
using System;
using System.Reflection;
using MessagePack.Internal;
using System.Linq;
using System.Linq.Expressions;

namespace MessagePack
{
    public interface IFormatterResolver
    {
        IMessagePackFormatter<T> GetFormatter<T>();
    }

    public static class FormatterResolverExtensions
    {
        static MethodInfo getFormatter;

        public static IMessagePackFormatter<T> GetFormatterWithVerify<T>(this IFormatterResolver resolver)
        {
            IMessagePackFormatter<T> formatter;
            try
            {
                formatter = resolver.GetFormatter<T>();
            }
            catch (TypeInitializationException ex)
            {
                Exception inner = ex;
                while (inner.InnerException != null)
                {
                    inner = inner.InnerException;
                }

                throw inner;
            }

            if (formatter == null)
            {
                throw new FormatterNotRegisteredException(typeof(T).FullName + " is not registered in this resolver. resolver:" + resolver.GetType().Name);
            }

            return formatter;
        }

        public static object GetFormatterDynamic(this IFormatterResolver resolver, Type type)
        {
            if (getFormatter == null)
            {
                getFormatter = typeof(IFormatterResolver).GetRuntimeMethod("GetFormatter", Type.EmptyTypes);
            }

            var formatter = getFormatter.MakeGenericMethod(type).Invoke(resolver, null);
            return formatter;
        }


#if NETSTANDARD1_4

        static readonly System.Collections.Concurrent.ConcurrentDictionary<ValueTuple<IFormatterResolver, Type>, ValueTuple<object, SerializeDynamicDelegate>>
            serializationCache = new System.Collections.Concurrent.ConcurrentDictionary<ValueTuple<IFormatterResolver, Type>, ValueTuple<object, SerializeDynamicDelegate>>();

        static readonly System.Collections.Concurrent.ConcurrentDictionary<ValueTuple<IFormatterResolver, Type>, ValueTuple<object, DeserializeDynamicDelegate>>
            deserializationCache = new System.Collections.Concurrent.ConcurrentDictionary<ValueTuple<IFormatterResolver, Type>, ValueTuple<object, DeserializeDynamicDelegate>>();

        public static int SerializeDynamic(this IFormatterResolver resolver, Type type, ref byte[] bytes, int offset, object value)
        {
            var serializer = serializationCache.GetOrAdd(ValueTuple.Create(resolver, type), _ =>
            {
                var formatter = resolver.GetFormatterDynamic(type);
                var formatterType = typeof(IMessagePackFormatter<>).MakeGenericType(type);

                // ((IMessagePackFormatter<T>)formatter).Serialize(ref bytes, offset, (T)value, formatterResolver);
                var arg0 = Expression.Parameter(typeof(object), "formatter");
                var arg1 = Expression.Parameter(typeof(byte[]).MakeByRefType(), "bytes");
                var arg2 = Expression.Parameter(typeof(int), "offset");
                var arg3 = Expression.Parameter(typeof(object), "value");
                var arg4 = Expression.Parameter(typeof(IFormatterResolver), "formatterResolver");

                var formatterExpr = Expression.Convert(arg0, formatterType);
                var valueExpr = type.GetTypeInfo().IsValueType ? Expression.Unbox(arg3, type) : Expression.Convert(arg3, type);

                var methodInfo = formatterType.GetRuntimeMethods().First(x => x.Name == "Serialize");

                var body = Expression.Call(formatterExpr, methodInfo, arg1, arg2, valueExpr, arg4);

                var lambda = Expression.Lambda<SerializeDynamicDelegate>(body, arg0, arg1, arg2, arg3, arg4);

                var method = lambda.Compile();

                return ValueTuple.Create(formatter, method);
            });

            return serializer.Item2.Invoke(serializer.Item1, ref bytes, offset, value, resolver);
        }

        public static object DeserializeDynamic(this IFormatterResolver resolver, Type type, byte[] bytes, int offset, out int readSize)
        {
            var deserializer = deserializationCache.GetOrAdd(ValueTuple.Create(resolver, type), _ =>
            {
                var formatter = resolver.GetFormatterDynamic(type);
                var formatterType = typeof(IMessagePackFormatter<>).MakeGenericType(type);

                // (object)((IMessagePackFormatter<T>)formatter).Deserialize(bytes, offset, formatterResolver, out readSize);
                var arg0 = Expression.Parameter(typeof(object), "formatter");
                var arg1 = Expression.Parameter(typeof(byte[]), "bytes");
                var arg2 = Expression.Parameter(typeof(int), "offset");
                var arg3 = Expression.Parameter(typeof(IFormatterResolver), "formatterResolver");
                var arg4 = Expression.Parameter(typeof(int).MakeByRefType(), "readSize");

                var formatterExpr = Expression.Convert(arg0, formatterType);
                var methodInfo = formatterType.GetRuntimeMethods().First(x => x.Name == "Deserialize");

                var call = Expression.Call(formatterExpr, methodInfo, arg1, arg2, arg3, arg4);
                var body = Expression.Convert(call, typeof(object));

                var lambda = Expression.Lambda<DeserializeDynamicDelegate>(body, arg0, arg1, arg2, arg3, arg4);

                var method = lambda.Compile();

                return ValueTuple.Create(formatter, method);
            });

            return deserializer.Item2.Invoke(deserializer.Item1, bytes, offset, resolver, out readSize);
        }

#endif

        public class FormatterNotRegisteredException : Exception
        {
            public FormatterNotRegisteredException(string message) : base(message)
            {
            }
        }
    }
}

#if NETSTANDARD1_4

namespace MessagePack.Internal
{
    public delegate int SerializeDynamicDelegate(object formatter, ref byte[] bytes, int offset, object value, IFormatterResolver formatterResolver);
    public delegate object DeserializeDynamicDelegate(object formatter, byte[] bytes, int offset, IFormatterResolver formatterResolver, out int readSize);
}

#endif