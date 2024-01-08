using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;

namespace _1brc;

class Program
{
    class Challenge
    {
        internal readonly MemoryMappedViewStream _viewStream;

        internal readonly long _fileLength;

        internal ConcurrentDictionary<string, string> map = new();
        
        public Challenge(string filename)
        {
            FileInfo fileInfo = new FileInfo(filename);
            _fileLength = fileInfo.Length;

            var mappedFile = MemoryMappedFile.CreateFromFile(filename, FileMode.Open);
            _viewStream = mappedFile.CreateViewStream();
        }

        unsafe List<(long, long)> split(string filename)
        {
            List<(long, long)> chunkOffsets = new List<(long, long)>();

            long maxChunkSize = _viewStream.Length / Environment.ProcessorCount;

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


        unsafe public void Run()
        {
            var chunks = split("./measurements.txt");

            byte* pointer = null;

            _viewStream.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
            
            chunks
#if !DEBUG
                .AsParallel()
#endif
                .Select(chunk =>
                {
                    long startPosition = chunk.Item1;
                    long chunkSize = chunk.Item2;

                    ReadOnlySpan<byte> chunkSpan = new ReadOnlySpan<byte>(pointer + startPosition, (int)chunkSize);

                    int start = 0;
                    int end = 0;

                    while (end < chunkSpan.Length)
                    {
                        while (end < chunkSpan.Length && chunkSpan[end] != '\n' && chunkSpan[end] != '\r')
                        {
                            end++;
                        }
                        var line = chunkSpan.Slice(start, end - start);

                        var separator = line.IndexOf((byte)';');

                        var city = line.Slice(0, separator);
                        var temperature = line.Slice(separator, line.Length - separator);
                        
                        
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

                    return chunk;
                }).ToList();
            
            _viewStream.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }


    static void Main(string[] args)
    {
        var challenge = new Challenge("./measurements.txt");

        challenge.Run();
    }
}