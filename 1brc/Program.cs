using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace _1brc;

class Program
{
    class Challenge
    {
        internal readonly MemoryMappedViewStream _viewStream;

        internal readonly long _fileLength;

        public Challenge(string filename)
        {
            FileInfo fileInfo = new FileInfo(filename);
            _fileLength = fileInfo.Length;

            var mappedFile = MemoryMappedFile.CreateFromFile(filename, FileMode.Open);
            _viewStream = mappedFile.CreateViewStream();
        }

        unsafe List<(long, long)> split()
        {
            List<(long, long)> chunkOffsets = new List<(long, long)>();

#if DEBUG
            long maxChunkSize = _viewStream.Length / 8;
#else
            long maxChunkSize = _viewStream.Length / Environment.ProcessorCount;
#endif

            long length = _viewStream.Length;
            byte* pointer = null;

            _viewStream.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);

            long position = 0;

            while (position < length)
            {
                long startPosition = position;
                long remainingLength = length - position;
                long chunkLength = Math.Min(maxChunkSize, remainingLength);

                ReadOnlySpan<byte> chunkSpan = new ReadOnlySpan<byte>(pointer + position, (int)chunkLength);

                int lastLineBreak = chunkSpan.LastIndexOf((byte)'\n');
                if (lastLineBreak != -1)
                {
                    position += lastLineBreak + 1;
                    chunkOffsets.Add((startPosition, position - startPosition));
                }
                else
                {
                    // No line break found within the chunk, consider the entire chunk as a line
                    position += chunkLength;
                    chunkOffsets.Add((startPosition, chunkLength));
                }
            }

            _viewStream.SafeMemoryMappedViewHandle.ReleasePointer();


            return chunkOffsets;
        }

        private const uint FnvPrime = 16777619;
        private const uint FnvOffsetBasis = 2166136261;

        unsafe struct UnsafeString : IEquatable<UnsafeString>
        {
            private readonly byte* pointer;
            private readonly int length;

            public UnsafeString(byte* pointer, int length)
            {
                this.pointer = pointer;
                this.length = length;
            }
        

        public ReadOnlySpan<byte> AsSpan() => new(pointer, length);

            public override string ToString()
            {
                return Encoding.UTF8.GetString(pointer, length);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Equals(UnsafeString other)
            {
                if (Sse2.IsSupported && length == 16 && other.length == 16)
                {
                    Vector128<byte> leftVector = Unsafe.ReadUnaligned<Vector128<byte>>(ref MemoryMarshal.GetReference(AsSpan()));
                    Vector128<byte> rightVector = Unsafe.ReadUnaligned<Vector128<byte>>(ref MemoryMarshal.GetReference(other.AsSpan()));

                    var equals = Sse2.CompareEqual(leftVector, rightVector);
                    var result = Sse2.MoveMask(equals);
                    return (result & 0xFFFF) == 0xFFFF;
                }

                return other.AsSpan().SequenceEqual(AsSpan());

            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public override bool Equals(object obj)
            {
                if (obj is UnsafeString other)
                {
                    return Equals(other);
                }

                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public override int GetHashCode()
            {
                
                if (length >= 3)
                    return (int)((length * 820243u) ^ (uint)(*(uint*)(pointer)));

                return (int)(uint)(*(ushort*)pointer * 31);
            }

            public static UnsafeString FromReadOnlySpan(ReadOnlySpan<byte> span)
            {
                fixed (byte* ptr = &span[0])
                {
                    return new UnsafeString(ptr, span.Length);
                }
            }
        }

        public nint ParseInt(ReadOnlySpan<byte> source)
        {
            nint sign;

            if (source[0] == (byte)'-')
            {
                sign = -1;
            }
            else
            {
                sign = 1;
            }

            if (source[1] == '.')
                return (nint)(source[0] * 10u + source[2] - ('0' * 11u)) * sign;

            return (nint)(source[0] * 100u + source[1] * 10 + source[3] - '0' * 111u) * sign;
        }


        unsafe Dictionary<UnsafeString, Aggregation> Compute((long, long) chunk, byte* pointer)
        {
            long startPosition = chunk.Item1;
            long chunkSize = chunk.Item2;

            ReadOnlySpan<byte> chunkSpan = new ReadOnlySpan<byte>(pointer + startPosition, (int)chunkSize);

            int start = 0;
            int end = 0;

            Dictionary<UnsafeString, Aggregation> localMap = new();

            while (end < chunkSpan.Length)
            {
                while (end < chunkSpan.Length && chunkSpan[end] != '\n' && chunkSpan[end] != '\r')
                {
                    end++;
                }

                var line = chunkSpan.Slice(start, end - start);

                ref byte spanRef = ref MemoryMarshal.GetReference(line);

                int separator = -1;
                for (int i = 4; i <= 7; i++)
                {
                    if (Unsafe.Add(ref spanRef, line.Length - i) == (byte)';')
                    {
                        separator = line.Length - i;
                        break;
                    }
                }

                var unsafeString = UnsafeString.FromReadOnlySpan(line.Slice(0, separator));

                var value = ParseInt(line.Slice(separator + 1, line.Length - separator - 1));
                ref var refVal = ref CollectionsMarshal.GetValueRefOrAddDefault(localMap, unsafeString, out bool exit);

                if (!exit)
                {
                    refVal = new Aggregation(value);
                }
                else
                {
                    refVal.Update(value);
                }

                if (end < chunkSpan.Length && (chunkSpan[end] == '\n' || chunkSpan[end] == '\r'))
                {
                    if (end + 1 < chunkSpan.Length && (chunkSpan[end] == '\n' && chunkSpan[end + 1] == '\r'))
                    {
                        end += 2;
                    }
                    else
                    {
                        end++;
                    }
                }

                start = end;
            }

            return localMap;
        }

        static object locker = new object();
        
        unsafe void AggregateResult(Dictionary<UnsafeString, Aggregation> input)
        {
            var kvArray = input.ToArray();
            var kvSpan = new Span<KeyValuePair<UnsafeString, Aggregation>>(kvArray);

            lock (locker)
            {
                fixed (KeyValuePair<UnsafeString, Aggregation>* ptr = kvSpan)
                {
                    var kvPtr = ptr;
                    var endPtr = kvPtr + kvSpan.Length;
                    while (kvPtr < endPtr)
                    {
                        if (!final.TryGetValue(kvPtr->Key, out Aggregation existing))
                        {
                            final[kvPtr->Key] = kvPtr->Value;
                        }
                        else
                        {
                            final[kvPtr->Key].Merge(kvPtr->Value);
                        }

                        kvPtr++;
                    }
                }   
            }
        }
        static Dictionary<UnsafeString, Aggregation> final = new(413);

        public unsafe void Run()
        {
            var chunks = split();

            var tasks = new List<Task>();

            byte* pointer = null;

            _viewStream.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);

            foreach (var chunk in chunks)
            {
                tasks.Add(Task.Run(() => Compute(chunk, pointer)).ContinueWith(task => AggregateResult(task.Result)));
            }

            Task
                .WhenAll(tasks)
                .GetAwaiter()
                .GetResult();

            //var stringBuilder = final.Aggregate(new StringBuilder(), (builder, kv) => builder.Append($"{kv.Key}={kv.Value},\n"));
            var stringBuilder = new StringBuilder();
            
            
            Console.OutputEncoding = Encoding.UTF8;
            foreach (var kv in final)
            {
                stringBuilder.Append($"{kv.Key}={kv.Value},\n");
            }
            Console.Write(stringBuilder);
            _viewStream.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }


    static void Main(string[] args)
    {
        var challenge = new Challenge("./measurements.txt");

        challenge.Run();
    }
}