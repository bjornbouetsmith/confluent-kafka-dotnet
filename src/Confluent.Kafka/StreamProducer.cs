using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Internal.Extensions;

namespace Confluent.Kafka
{
    /// <summary>
    /// Producer that serializes to memory streams for more efficent use of memory.
    /// Expects <see cref="MemoryStream"/>s to be assigned to the <see cref="StreamMessage{TKey, TValue}"/> published
    /// </summary>
    internal class StreamProducer<TKey, TValue> : Producer<TKey, TValue>, IStreamProducer<TKey, TValue>
    {
        private IStreamSerializer<TKey> keyStreamSerializer;
        private IStreamSerializer<TValue> valueStreamSerializer;

        internal StreamProducer(StreamProducerBuilder<TKey, TValue> builder) : base(builder, false)
        {
            InitializeSerializers(builder.KeyStreamSerializer, builder.ValueStreamSerializer);
        }

        private void InitializeSerializers(
            IStreamSerializer<TKey> keyStreamSerializer,
            IStreamSerializer<TValue> valueStreamSerializer)
        {
            // setup key serializer.
            if (keyStreamSerializer == null)
            {
                if (!Serializers.TryGetStreamSerializer(out this.keyStreamSerializer))
                {
                    throw new ArgumentNullException(
                        $"Key serializer not specified and there is no default serializer defined for type {typeof(TKey).Name}.");
                }
            }
            else
            {
                this.keyStreamSerializer = keyStreamSerializer;
            }


            // setup value serializer.
            if (valueStreamSerializer == null)
            {
                if (!Serializers.TryGetStreamSerializer(out this.valueStreamSerializer))
                {
                    throw new ArgumentNullException(
                        $"Value serializer not specified and there is no default serializer defined for type {typeof(TValue).Name}.");
                }
            }
            else
            {
                this.valueStreamSerializer = valueStreamSerializer;
            }
        }

        public Task<DeliveryResult<TKey, TValue>> ProduceAsync(string topic, StreamMessage<TKey, TValue> message, CancellationToken cancellationToken = default(CancellationToken))
            => ProduceAsync(new TopicPartition(topic, Partition.Any), message, cancellationToken);

        /// <summary>
        /// Represents the payload that has to be passed onto the underlying library.
        /// The array segment contains the serialized bytes and the offset and length within the byte array
        /// </summary>
        private readonly struct Payload
        {
            /// <summary>
            /// The key of the message converted into an arraysegment with offset/count
            /// </summary>
            public ArraySegment<byte> Key { get; }

            /// <summary>
            /// The value of the message converted into an arraysegment with offset/count
            /// </summary>
            public ArraySegment<byte> Value { get; }

            public Payload(ArraySegment<byte> key, ArraySegment<byte> value)
            {
                Key = key;
                Value = value;
            }
        }

        //[ThreadStatic]
        //private static MemoryStream _valueStream;
        //[ThreadStatic]
        //private static MemoryStream _keyStream;

        private async Task<Payload> GetPayloadAsync(Headers headers, TopicPartition topicPartition, StreamMessage<TKey, TValue> message, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (message.KeyStream.Position == 0)
            {
                try
                {
                    await this.keyStreamSerializer.SerializeAsync(message.Key, message.KeyStream, new SerializationContext(MessageComponentType.Key, topicPartition.Topic, headers), cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw new ProduceException<TKey, TValue>(
                        new Error(ErrorCode.Local_KeySerialization),
                        new DeliveryResult<TKey, TValue>
                        {
                            Message = message,
                            TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                        },
                        ex);
                }
            }

            if (message.ValueStream.Position == 0)
            {
                try
                {
                    await this.valueStreamSerializer.SerializeAsync(message.Value, message.ValueStream, new SerializationContext(MessageComponentType.Value, topicPartition.Topic, headers), cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw new ProduceException<TKey, TValue>(
                        new Error(ErrorCode.Local_ValueSerialization),
                        new DeliveryResult<TKey, TValue>
                        {
                            Message = message,
                            TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                        },
                        ex);
                }
            }
            
            return GetPayloadAfterSerialization(message);
        }

        private Payload GetPayload(Headers headers, TopicPartition topicPartition, StreamMessage<TKey, TValue> message)
        {
            //var keyStream = _keyStream ?? (_keyStream = new MemoryStream());
            //var valueStream = _valueStream ?? (_valueStream = new MemoryStream());
            //keyStream.Position = 0;
            //valueStream.Position = 0;
            if (message.KeyStream.Position == 0)
            {
                try
                {
                    this.keyStreamSerializer.Serialize(message.Key, message.KeyStream, new SerializationContext(MessageComponentType.Key, topicPartition.Topic, headers));
                }
                catch (Exception ex)
                {
                    throw new ProduceException<TKey, TValue>(
                        new Error(ErrorCode.Local_KeySerialization),
                        new DeliveryResult<TKey, TValue>
                        {
                            Message = message,
                            TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                        },
                        ex);
                }
            }

            if (message.ValueStream.Position == 0)
            {
                try
                {
                    this.valueStreamSerializer.Serialize(message.Value, message.ValueStream, new SerializationContext(MessageComponentType.Value, topicPartition.Topic, headers));
                }
                catch (Exception ex)
                {
                    throw new ProduceException<TKey, TValue>(
                        new Error(ErrorCode.Local_ValueSerialization),
                        new DeliveryResult<TKey, TValue>
                        {
                            Message = message,
                            TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                        },
                        ex);
                }
            }

            return GetPayloadAfterSerialization(message);
        }

        /// <summary>
        /// Converts the streams on the message into a <see cref="Payload"/> where offset/count is correctly set
        /// </summary>
        private Payload GetPayloadAfterSerialization(StreamMessage<TKey, TValue> message)
        {
            ArraySegment<byte> keySegment = message.KeyStream.Position > 0 ? message.KeyStream.GetBufferAsArraySegment() : default;
            var keyLength = (int)message.KeyStream.Position;
            keySegment = keySegment.Array != null ? new ArraySegment<byte>(keySegment.Array, keySegment.Offset, keyLength) : default;

            ArraySegment<byte> valueSegment = message.ValueStream.Position > 0 ? message.ValueStream.GetBufferAsArraySegment() : default;
            var valLength = (int)message.ValueStream.Position;

            valueSegment = valueSegment.Array != null ? new ArraySegment<byte>(valueSegment.Array, valueSegment.Offset, valLength) : default;

            return new Payload(keySegment, valueSegment);

        }

        public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(TopicPartition topicPartition, StreamMessage<TKey, TValue> message, CancellationToken cancellationToken = default(CancellationToken))
        {
            Headers headers = message.Headers ?? new Headers();

            var payload = await GetPayloadAsync(headers, topicPartition, message, cancellationToken).ConfigureAwait(false);

            try
            {
                if (enableDeliveryReports)
                {
                    var handler = new TypedTaskDeliveryHandlerShim(
                        topicPartition.Topic,
                        enableDeliveryReportKey ? message.Key : default(TKey),
                        enableDeliveryReportValue ? message.Value : default(TValue));

                    if (cancellationToken.CanBeCanceled)
                    {
                        handler.CancellationTokenRegistration
                            = cancellationToken.Register(() => handler.TrySetCanceled());
                    }


                    ProduceImpl(
                        topicPartition.Topic,
                        payload.Value.Array, payload.Value.Offset, payload.Value.Count,
                        payload.Key.Array, payload.Key.Offset, payload.Key.Count,
                        message.Timestamp, topicPartition.Partition, headers.BackingList,
                        handler);

                    return await handler.Task.ConfigureAwait(false);
                }
                else
                {
                    ProduceImpl(
                        topicPartition.Topic,
                        payload.Value.Array, payload.Value.Offset, payload.Value.Count,
                        payload.Key.Array, payload.Key.Offset, payload.Key.Count,
                        message.Timestamp, topicPartition.Partition, headers.BackingList,
                        null);

                    var result = new DeliveryResult<TKey, TValue>
                    {
                        TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset),
                        Message = message
                    };

                    return result;
                }
            }
            catch (KafkaException ex)
            {
                throw new ProduceException<TKey, TValue>(
                    ex.Error,
                    new DeliveryResult<TKey, TValue>
                    {
                        Message = message,
                        TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                    });
            }
        }

        public void Produce(
            string topic,
            StreamMessage<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
            => Produce(new TopicPartition(topic, Partition.Any), message, deliveryHandler);

        public void Produce(
            TopicPartition topicPartition,
            StreamMessage<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            if (deliveryHandler != null && !enableDeliveryReports)
            {
                throw new InvalidOperationException("A delivery handler was specified, but delivery reports are disabled.");
            }

            Headers headers = message.Headers ?? new Headers();

            var payload = GetPayload(headers, topicPartition, message);

            try
            {
                ProduceImpl(
                    topicPartition.Topic,
                    payload.Value.Array, payload.Value.Offset, payload.Value.Count,
                    payload.Key.Array, payload.Key.Offset, payload.Key.Count,
                    message.Timestamp, topicPartition.Partition,
                    headers.BackingList,
                    deliveryHandler == null
                        ? null
                        : new TypedDeliveryHandlerShim_Action(
                            topicPartition.Topic,
                            enableDeliveryReportKey ? message.Key : default(TKey),
                            enableDeliveryReportValue ? message.Value : default(TValue),
                            deliveryHandler));
            }
            catch (KafkaException ex)
            {
                throw new ProduceException<TKey, TValue>(
                    ex.Error,
                    new DeliveryReport<TKey, TValue>
                    {
                        Message = message,
                        TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                    });
            }
        }
    }
}