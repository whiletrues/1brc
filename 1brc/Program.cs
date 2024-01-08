using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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

            long maxChunkSize = _viewStream.Length / 8;

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
        
        static int GetHashCode(ReadOnlySpan<byte> span)
        {
            int hashCode = 0;
            for (int i = 0; i < span.Length; i++)
                hashCode = 31 * hashCode + span[i];
            return hashCode;
        }

        unsafe Dictionary<int, string> Compute((long, long) chunk, byte* pointer)
        {
            
            long startPosition = chunk.Item1;
            long chunkSize = chunk.Item2;

            ReadOnlySpan<byte> chunkSpan = new ReadOnlySpan<byte>(pointer + startPosition, (int)chunkSize);

            int start = 0;
            int end = 0;

            Dictionary<int, string> localMap = new();
            
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
                
                var city = line.Slice(0, separator).ToArray();
                var temperature = line.Slice(separator + 1, line.Length - separator - 1);
                
                // Convert the input string to a byte array and compute the hash.
                var key = GetHashCode(city);
                
                ref var refVal = ref CollectionsMarshal.GetValueRefOrAddDefault(localMap, key, out bool exit);
                if (!exit)
                {
                    refVal = Encoding.UTF8.GetString(temperature);
                }

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

        public unsafe void Run()
        {
            var chunks = split();

            var tasks = new List<Task>(chunks.Count);

            byte* pointer = null;

            _viewStream.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);

            foreach (var chunk in chunks)
            {
                var task = Task.Run(() => Compute(chunk, pointer));
                tasks.Add(task);
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