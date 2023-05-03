using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka
{
    /// <summary>
    /// Defines a serializer that serializes to a memory stream for efficient buffer re-use
    /// </summary>
    public interface IStreamSerializer<T>
    {
        /// <summary>
        /// Serializes the give data to the targetstream
        /// </summary>
        /// <param name="data">The data to serialize</param>
        /// <param name="targetStream">The target stream where to write the serialized data</param>
        /// <param name="context">Context relevant to the serialize operation.</param>
        void Serialize(T data, MemoryStream targetStream, SerializationContext context);

        /// <summary>
        /// Serializes the give data to the targetstream
        /// </summary>
        /// <param name="data">The data to serialize</param>
        /// <param name="targetStream">The target stream where to write the serialized data</param>
        /// <param name="context">Context relevant to the serialize operation.</param>
        /// <param name="cancellationToken">A cancellation token to observe whilst waiting for the returned task to complete.</param>
        /// <returns>A task that completes when serialization is complete</returns>
        Task SerializeAsync(T data, MemoryStream targetStream, SerializationContext context, CancellationToken cancellationToken = default);
    }
}