using System;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka
{
    /// <summary>
    /// Defines a producer that serializes to memory streams for more efficent use of memory.
    /// <see cref="StreamMessage{TKey, TValue}"/> inherits from <see cref="Message{TKey, TValue}"/> but also exposes two member streams
    /// One for the key and one for the value.
    /// The producer should expect these to be non-null and use an instance of a <see cref="IStreamSerializer{T}"/> to serialize both key and value to their
    /// respective streams. If the streams positions are non-zero no serialization should be attempted, since its a signal that the streams should be used as
    /// the key/value and passed on directly to the underlying kafka library as the serialized key/value.
    /// </summary>
    public interface IStreamProducer<TKey, TValue> : IProducer<TKey, TValue>
    {
        /// <summary>
        /// <see cref="IProducer{TKey, TValue}.ProduceAsync(string, Message{TKey, TValue}, CancellationToken)"/>
        /// </summary>
        Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            string topic,
            StreamMessage<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// <see cref="IProducer{TKey, TValue}.ProduceAsync(TopicPartition, Message{TKey, TValue}, CancellationToken)"/>
        /// </summary>
        Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            TopicPartition topicPartition,
            StreamMessage<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken));


        /// <summary>
        /// <see cref="IProducer{TKey, TValue}.Produce(string, Message{TKey, TValue}, Action{DeliveryReport{TKey, TValue}})"/>
        /// </summary>
        void Produce(
            string topic,
            StreamMessage<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null);

        /// <summary>
        /// <see cref="IProducer{TKey, TValue}.Produce(TopicPartition, Message{TKey, TValue},Action{DeliveryReport{TKey, TValue}})"/>
        /// </summary>
        void Produce(
            TopicPartition topicPartition,
            StreamMessage<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null);
    }
}