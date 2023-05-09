using System;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka
{
    /// <summary>
    /// Defines a serializer that serializes to a <see cref="ReadOnlyMemory{byte}"/> which allow imlementation that re-use buffers
    /// </summary>
    public interface IMemorySerializer<T>
    {
        /// <summary>
        /// Serializes the given data and returns a readonly memory of bytes
        /// </summary>
        /// <param name="data">The data to serialize</param>
        /// <param name="context">Context relevant to the serialize operation.</param>
        ReadOnlyMemory<byte> Serialize(T data, SerializationContext context);

        /// <summary>
        /// Serializes the given data and returns a readonly memory of bytes
        /// </summary>
        /// <param name="data">The data to serialize</param>
        /// <param name="context">Context relevant to the serialize operation.</param>
        /// <param name="cancellationToken">A cancellation token to observe whilst waiting for the returned task to complete.</param>
        /// <returns>A task that completes when serialization is complete with the serialized data</returns>
        Task<ReadOnlyMemory<byte>> SerializeAsync(T data, SerializationContext context, CancellationToken cancellationToken = default);
    }
}