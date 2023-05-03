using System;
using System.IO;

namespace Confluent.Kafka.Internal.Extensions
{
    internal static class MemoryStreamExtensions
    {
        internal static ArraySegment<byte> GetBufferAsArraySegment(this MemoryStream memoryStream)
        {
            if (memoryStream.TryGetBuffer(out var arraySegment))
            {
                return arraySegment;
            }
            // Stream was created from existing byte array and disallow exposing array and cannot be accessed efficiently
            var buffer = memoryStream.ToArray();
            return new ArraySegment<byte>(buffer);

        }
    }
}
