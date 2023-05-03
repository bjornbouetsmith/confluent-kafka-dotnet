using System.IO;

namespace Confluent.Kafka
{
    /// <summary>
    /// Represents a message where producers have assigned memory streams
    /// to allow for efficient memory management and prevent allocation of byte arrays on every serialization.
    /// Compared to <see cref="Message{TKey, TValue}"/> this message type is meant to be either fully or partially re-used between publish attempts.
    /// And at a minimum the the usage should be to re-use the <see cref="ValueStream"/> &amp; <see cref="KeyStream"/>
    /// Producers that use this type expects both <see cref="ValueStream"/> &amp; <see cref="KeyStream"/> to be assigned to a valid instance.
    /// If the memory streams positions are non-zero the key and value are assumed to have already been serialized to the given streams and the producers should not
    /// try to serialize them.
    /// </summary>
    public class StreamMessage<TKey,TValue> : Message<TKey, TValue>
    {
        /// <summary>
        /// The stream to which serializers will serialize the value of this message
        /// </summary>
        public MemoryStream ValueStream { get; set; }
        
        /// <summary>
        /// The stream to which serializers will serialize the key of this message
        /// </summary>
        public MemoryStream KeyStream { get; set; }
    }
}