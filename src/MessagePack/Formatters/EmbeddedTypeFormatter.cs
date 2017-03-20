using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace MessagePack.Formatters
{
    public class EmbeddedTypeFormatter : IMessagePackFormatter<object>
    {
        static readonly Regex SubtractFullNameRegex = new Regex(@", Version=\d+.\d+.\d+.\d+, Culture=\w+, PublicKeyToken=\w+", RegexOptions.Compiled);

        readonly HashSet<string> blacklistCheck;

        public EmbeddedTypeFormatter(string[] blacklist)
        {
            blacklistCheck = new HashSet<string>(blacklist ?? new string[0]);
            blacklistCheck.Add("System.CodeDom.Compiler.TempFileCollection");
            blacklistCheck.Add("System.IO.FileSystemInfo");
            blacklistCheck.Add("System.Management.IWbemClassObjectFreeThreaded");
        }

        // see:http://msdn.microsoft.com/en-us/library/w3f99sx1.aspx
        // subtract Version, Culture and PublicKeyToken from AssemblyQualifiedName 
        internal static string BuildTypeName(Type type)
        {
            return SubtractFullNameRegex.Replace(type.AssemblyQualifiedName, "");
        }

        public int Serialize(ref byte[] bytes, int offset, object value, IFormatterResolver formatterResolver)
        {
            if (value == null)
            {
                return MessagePackBinary.WriteNil(ref bytes, offset);
            }

            var t = value.GetType();
            var typeName = BuildTypeName(t);

            if (blacklistCheck.Contains(typeName))
            {
                throw new InvalidOperationException("Type is in blacklist:" + typeName);
            }

            var startOffset = offset;

            offset += MessagePackBinary.WriteArrayHeader(ref bytes, offset, 2);
            offset += MessagePackBinary.WriteString(ref bytes, offset, typeName);
            offset += formatterResolver.SerializeDynamic(t, ref bytes, offset, value);

            return offset - startOffset;
        }

        public object Deserialize(byte[] bytes, int offset, IFormatterResolver formatterResolver, out int readSize)
        {
            if (MessagePackBinary.IsNil(bytes, offset))
            {
                readSize = 1;
                return null;
            }

            var startOffset = offset;

            var len = MessagePackBinary.ReadArrayHeader(bytes, offset, out readSize);
            offset += readSize;
            if (len != 2)
            {
                throw new InvalidOperationException("Invalid format");
            }

            var typeName = MessagePackBinary.ReadString(bytes, offset, out readSize);
            offset += readSize;

            var t = Type.GetType(typeName, false);
            if (t == null)
            {
                throw new InvalidOperationException("Can't find type:" + typeName);
            }

            var value = formatterResolver.DeserializeDynamic(t, bytes, offset, out readSize);
            offset += readSize;

            readSize = offset - startOffset;
            return value;
        }
    }
}