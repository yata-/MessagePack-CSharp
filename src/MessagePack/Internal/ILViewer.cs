using System.IO;
using System.Reflection;
using System.Reflection.Emit;

namespace MessagePack.Internal
{
    internal class ILStreamReader : BinaryReader
    {
        static readonly OpCode[] oneByteOpCodes = new OpCode[0x100];
        static readonly OpCode[] twoByteOpCodes = new OpCode[0x100];

        int endPosition;

        public int CurrentPosition { get { return (int)BaseStream.Position; } }

        public bool EndOfStream { get { return !((int)BaseStream.Position < endPosition); } }

        static ILStreamReader()
        {
            foreach (var fi in typeof(OpCodes).GetFields(BindingFlags.Public | BindingFlags.Static))
            {
                var opCode = (OpCode)fi.GetValue(null);
                var value =  unchecked((ushort)opCode.Value);

                if (value < 0x100)
                {
                    oneByteOpCodes[value] = opCode;
                }
                else if ((value & 0xff00) == 0xfe00)
                {
                    twoByteOpCodes[value & 0xff] = opCode;
                }
            }
        }

        public ILStreamReader(byte[] ilByteArray)
            : base(new MemoryStream(ilByteArray))
        {
            this.endPosition = ilByteArray.Length;
        }

        public OpCode ReadOpCode()
        {
            var code = ReadByte();
            if (code != 0xFE)
            {
                return oneByteOpCodes[code];
            }
            else
            {
                code = ReadByte();
                return twoByteOpCodes[code];
            }
        }

        public int ReadMetadataToken()
        {
            return ReadInt32();
        }
    }
}