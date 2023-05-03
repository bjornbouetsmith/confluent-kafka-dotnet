using JetBrains.dotMemoryUnit;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

[assembly: SuppressXUnitOutputException]

namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        public static IEnumerable<object[]> StreamTestParameters()
        {
            var bootstrapServerts = (string)KafkaParameters().First()[0];

            var runId = Guid.NewGuid();
            var topic = "memorystream_test" + runId;

            yield return new object[] { bootstrapServerts, topic, false };
            yield return new object[] { bootstrapServerts, topic, true };
        }

        [Theory, MemberData(nameof(StreamTestParameters))]
        public void ProduceBuiltInSerializerTypesString(string bootstrapServers, string topic, bool preSerialize)
        {
            var key = Guid.NewGuid().ToString();
            var value = "value" + key;

            ProduceTest(bootstrapServers, topic, key, value, preSerialize);
        }

        [Theory, MemberData(nameof(StreamTestParameters))]
        public void ProduceBuiltInSerializerTypesInt(string bootstrapServers, string topic, bool preSerialize)
        {

            var key = int.MinValue / 2;
            var value = int.MaxValue;

            ProduceTest(bootstrapServers, topic, key, value, preSerialize);
        }

        [Theory, MemberData(nameof(StreamTestParameters))]
        public void ProduceBuiltInSerializerTypesLong(string bootstrapServers, string topic, bool preSerialize)
        {

            var key = long.MinValue / 2;
            var value = long.MaxValue / 2;

            ProduceTest(bootstrapServers, topic, key, value, preSerialize);
        }

        [Theory, MemberData(nameof(StreamTestParameters))]
        public void ProduceBuiltInSerializerTypesSingle(string bootstrapServers, string topic, bool preSerialize)
        {

            var key = float.MinValue / 2;
            var value = float.MaxValue / 2;

            ProduceTest(bootstrapServers, topic, key, value, preSerialize);
        }

        [Theory, MemberData(nameof(StreamTestParameters))]
        public void ProduceBuiltInSerializerTypesDouble(string bootstrapServers, string topic, bool preSerialize)
        {

            var key = double.MinValue / 2;
            var value = double.MaxValue / 2;

            ProduceTest(bootstrapServers, topic, key, value, preSerialize);
        }

        [Theory, MemberData(nameof(StreamTestParameters))]
        public void ProduceBuiltInSerializerTypesByteArray(string bootstrapServers, string topic, bool preSerialize)
        {

            var key = "mstest" + Guid.NewGuid();
            var value = Encoding.UTF8.GetBytes("Byte array serialization works");

            ProduceTest(bootstrapServers, topic, key, value, preSerialize);
        }
        
        [Theory, MemberData(nameof(StreamTestParameters))]
        public void ProduceCustomValueSerializer(string bootstrapServers, string topic, bool preSerialize)
        {

            var key = "mstest" + Guid.NewGuid();
            var value = new DummyClass();



            ProduceTest(bootstrapServers, topic, key, value, preSerialize,
                b => b.SetValueSerializer(new NaiveSerializer<DummyClass>(new byte[] { 1, 2, 3, 4 }))
                );
        }

        
        [DotMemoryUnit(FailIfRunWithoutSupport = false, CollectAllocations = true)]
        [Theory, MemberData(nameof(StreamTestParameters))]
        public void StreamProducerWithClassPreSerialized(string bootstrapServers, string topic, bool preSerialize)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            var naiveSerializer = new NaiveSerializer<DummyClass>(new byte[] { 1, 2, 3, 4 });
            var producerBuilder = new StreamProducerBuilder<string, DummyClass>(producerConfig);

            producerBuilder.SetValueSerializer(naiveSerializer);


            using var producer = producerBuilder.BuildStreamProducer();

            var message = new StreamMessage<string, DummyClass>
            {
                KeyStream = new MemoryStream(),
                ValueStream = new MemoryStream()
            };
            Serializers.TryGetStreamSerializer<string>(out var keySerializer);
            keySerializer.Serialize("Hello", message.KeyStream, default);
            naiveSerializer.Serialize(new DummyClass(), message.ValueStream, default);

            producer.Produce(topic, message);

        }


        private static void ProduceTest<TKey, TValue>(string bootstrapServers, string topic, TKey expectedKey, TValue expectedValue, bool preSerialize, Func<StreamProducerBuilder<TKey, TValue>, StreamProducerBuilder<TKey, TValue>> builderWork = null)
        {
            RunWithCleanUp(
                bootstrapServers,
                topic,
                () =>
                {
                    var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
                    var producerBuilder = new StreamProducerBuilder<TKey, TValue>(producerConfig);
                    if (builderWork != null)
                    {
                        producerBuilder = builderWork(producerBuilder);
                    }

                    using var producer = producerBuilder.BuildStreamProducer();

                    var message = new StreamMessage<TKey, TValue>
                    {
                        KeyStream = new MemoryStream(),
                        ValueStream = new MemoryStream(),
                        Key = expectedKey,
                        Value = expectedValue
                    };

                    if (preSerialize)
                    {
                        Serializers.TryGetStreamSerializer<TKey>(out var keySerializer);
                        keySerializer.Serialize(expectedKey, message.KeyStream, default);

                        Serializers.TryGetStreamSerializer<TValue>(out var valueSerializer);
                        valueSerializer.Serialize(expectedValue, message.ValueStream, default);
                    }

                    ManualResetEvent published = new ManualResetEvent(false);
                    producer.Produce(topic, message, rp => { published.Set(); });
                    published.WaitOne(TimeSpan.FromSeconds(10));
                    if (builderWork == null)
                    {
                        Consume(bootstrapServers, topic, message.Key, message.Value);
                    }
                });
        }


        private static void Consume<TKey, TValue>(string bootstrapServers, string topic, TKey expectedKey, TValue expectedValue)
        {
            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<TKey, TValue>(consumerConfig).Build();
            consumer.Subscribe(topic);

            var r = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.NotNull(r);
            Assert.Equal(expectedKey, r.Message.Key);
            Assert.Equal(expectedValue, r.Message.Value);

            consumer.Close();
        }

        private static void RunWithCleanUp(string bootstrapServers, string topic, Action action)
        {
            try
            {
                action();
            }
            finally
            {
                var clientBuilder = new AdminClientBuilder(new AdminClientConfig
                {
                    BootstrapServers = bootstrapServers
                });

                using var client = clientBuilder.Build();
                client.DeleteTopicsAsync(new[] { topic });
            }
        }


        // Test class just to have a type to expect a serializer for
        public class DummyClass
        {

        }

        public class NaiveSerializer<T> : ISerializer<T>, IStreamSerializer<T>, IDeserializer<T> where T : new()
        {
            private readonly byte[] bytes;

            public NaiveSerializer(byte[] bytes)
            {
                this.bytes = bytes;
            }

            public byte[] Serialize(T data, SerializationContext context)
            {
                // Simulate a non performance way of serializing because it allocates a byte array
                var target = new byte[this.bytes.Length];
                Array.Copy(this.bytes, target, this.bytes.Length);
                return target;
            }

            public void Serialize(T data, MemoryStream targetStream, SerializationContext context)
            {
                targetStream.Write(this.bytes, 0, this.bytes.Length);
            }

            public Task SerializeAsync(T data, MemoryStream targetStream, SerializationContext context, CancellationToken cancellationToken = default)
            {
                Serialize(data, targetStream, context);
                return Task.CompletedTask;
            }

            public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                return new T();
            }
        }

    }
}
