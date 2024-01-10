using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Security.Cryptography;
using System.Text;

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

        public unsafe int ParseInt(ReadOnlySpan<byte> source, int start, int length)
        {
            int sign = 1;
            uint value = 0;
            var end = start + length;

            fixed (byte* sourcePtr = &source[0])
            {
                byte* ptr = sourcePtr + start;

                for (; start < end; start++)
                {
                    var c = (uint)*(ptr + start);

                    if (c == '-')
                        sign = -1;
                    else
                        value = value * 10u + (c - '0');
                }

                var fractional = (uint)*(ptr + start + 1) - '0';
                return sign * (int)(value * 10 + fractional);
            }
        }

        private const uint FnvPrime = 16777619;
        private const uint FnvOffsetBasis = 2166136261;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int GetHashCode(ReadOnlySpan<byte> span)
        {
            unchecked
            {
                uint hash = FnvOffsetBasis;

                fixed (byte* ptr = &span[0])
                {
                    byte* p = ptr;
                    int remainingBytes = span.Length;

                    while (remainingBytes >= sizeof(uint))
                    {
                        hash ^= *((uint*)p);
                        hash *= FnvPrime;

                        p += sizeof(uint);
                        remainingBytes -= sizeof(uint);
                    }

                    if (remainingBytes > 0)
                    {
                        uint remainingValue = 0;

                        for (int i = 0; i < remainingBytes; ++i)
                        {
                            remainingValue <<= 8;
                            remainingValue |= *p++;
                        }

                        hash ^= remainingValue;
                        hash *= FnvPrime;
                    }
                }

                return (int)hash;
            }
        }

        unsafe Dictionary<int, int> Compute((long, long) chunk, byte* pointer)
        {
            long startPosition = chunk.Item1;
            long chunkSize = chunk.Item2;

            ReadOnlySpan<byte> chunkSpan = new ReadOnlySpan<byte>(pointer + startPosition, (int)chunkSize);

            int start = 0;
            int end = 0;

            Dictionary<int, int> localMap = new();

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

                var city = line.Slice(0, separator);


                ref var refVal =
                    ref CollectionsMarshal.GetValueRefOrAddDefault(localMap, GetHashCode(city), out bool exit);
                if (!exit)
                {
                    refVal = ParseInt(line, separator + 1, line.Length - separator - 1);
                }


                // Convert the input string to a byte array and compute the hash.
                // Move to the next line
                if (end < chunkSpan.Length && (chunkSpan[end] == '\n' || chunkSpan[end] == '\r'))
                {
                    // Move past the newline character(s)
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


        static Dictionary<int, int> result = new(100000);


        unsafe void AggregateResult(Dictionary<int, int> input)
        {

            var kvArray = input.ToArray();
            var kvSpan = new Span<KeyValuePair<int, int>>(kvArray);

            fixed (KeyValuePair<int, int>* ptr = kvSpan)
            {
                var kvPtr = ptr;
                var endPtr = kvPtr + kvSpan.Length;
                while (kvPtr < endPtr)
                {
                    if (!result.TryGetValue(kvPtr->Key, out int existingValue))
                    {
                        result[kvPtr->Key] = kvPtr->Value;
                    }
                    else
                    {
                        result[kvPtr->Key] = existingValue + kvPtr->Value;
                    }

                    kvPtr++;
                }
            }
        }

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


            _viewStream.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }


    static void Main(string[] args)
    {
        var challenge = new Challenge("./measurements.txt");

        challenge.Run();
    }
}