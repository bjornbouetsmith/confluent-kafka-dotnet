using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.StreamBenchmark
{
    //[SimpleJob(RuntimeMoniker.Net462, baseline: true)]
    [SimpleJob(RuntimeMoniker.Net60)]
    //[SimpleJob(RuntimeMoniker.NativeAot70)]
    [RPlotExporter]
    [MemoryDiagnoser]
    public class StreamVsOld
    {
        private byte[] data;
        private string stringValue;
        private IStreamProducer<int, byte[]> streamProducerByteArray;
        private IProducer<int, byte[]> producerByteArray;
        private string topic;
        private IStreamProducer<int, string> streamProducerString;
        private IProducer<int, string> producerString;
        private IProducer<int, double> producerDouble;
        private IProducer<int, DummyClass> producerClass;

        private Message<int, byte[]> byteMessage;
        private Message<int, string> stringMessage;
        private Message<int, double> doubleMessage;
        private Message<int, DummyClass> classMessage;

        private StreamMessage<int, byte[]> byteStreamMessage;
        private StreamMessage<int, string> stringStreamMessage;
        private StreamMessage<int, double> doubleStreamMessage;
        private StreamMessage<int, DummyClass> classStreamMessage;
        private StreamMessage<int, DummyClass> classStreamMessageSerialized;

        private IStreamProducer<int, double> streamProducerDouble;
        private IStreamProducer<int, DummyClass> streamProducerClass;
        private TopicPartition topicPartition;

        [GlobalSetup]
        public void Setup()
        {
            topic = "StreamBenchMark_" + Guid.NewGuid();
            topicPartition = new TopicPartition(topic, Partition.Any);
            data = new byte[250];
            stringValue = "HelloWorldឆ្មាត្រូវបានហែលទឹកвы не бананHelloWorldឆ្មាត្រូវបានហែលទឹកвы не бананHelloWorldឆ្មាត្រូវបានហែលទឹកвы не банан";
            new Random(42).NextBytes(data);
            var producerConfig = new ProducerConfig { BootstrapServers = "192.168.2.101:9092", QueueBufferingMaxMessages = 0, QueueBufferingMaxKbytes = 1024 * 1024 * 1024, EnableDeliveryReports = false }; // Just set limits high, so we don't get queue full exceptions
            streamProducerByteArray = new StreamProducerBuilder<int, byte[]>(producerConfig).BuildStreamProducer();
            producerByteArray = new ProducerBuilder<int, byte[]>(producerConfig).Build();
            streamProducerString = new StreamProducerBuilder<int, string>(producerConfig).BuildStreamProducer();
            streamProducerDouble = new StreamProducerBuilder<int, double>(producerConfig).BuildStreamProducer();
            var naiveSerializer = new NaiveSerializer<DummyClass>(data);
            streamProducerClass = new StreamProducerBuilder<int, DummyClass>(producerConfig).SetValueSerializer(naiveSerializer).BuildStreamProducer();
            producerString = new ProducerBuilder<int, string>(producerConfig).Build();
            producerDouble = new ProducerBuilder<int, double>(producerConfig).Build();
            producerClass = new ProducerBuilder<int, DummyClass>(producerConfig).SetValueSerializer(naiveSerializer).Build();

            byteMessage = new Message<int, byte[]> { Headers = new Headers(), Key = 123, Value = data };
            stringMessage = new Message<int, string> { Headers = new Headers(), Key = 123, Value = stringValue };
            doubleMessage = new Message<int, double> { Headers = new Headers(), Key = 123, Value = double.MaxValue / 2 - 1.23456789d };
            classMessage = new Message<int, DummyClass> { Headers = new Headers(), Key = 123, Value = new DummyClass() };
            byteStreamMessage = new StreamMessage<int, byte[]> { KeyStream = new MemoryStream(), ValueStream = new MemoryStream(200), Headers = new Headers(), Key = 123, Value = data };
            stringStreamMessage = new StreamMessage<int, string> { KeyStream = new MemoryStream(), ValueStream = new MemoryStream(200), Headers = new Headers(), Key = 123, Value = stringValue };
            doubleStreamMessage = new StreamMessage<int, double> { KeyStream = new MemoryStream(), ValueStream = new MemoryStream(200), Headers = new Headers(), Key = 123, Value = double.MaxValue / 2 - 1.23456789d };
            classStreamMessage = new StreamMessage<int, DummyClass> { KeyStream = new MemoryStream(), ValueStream = new MemoryStream(200), Headers = new Headers(), Key = 123, Value = new DummyClass() };
            classStreamMessageSerialized = new StreamMessage<int, DummyClass> { KeyStream = new MemoryStream(), ValueStream = new MemoryStream(200), Headers = new Headers(), Key = 123, Value = new DummyClass() };

            Serializers.TryGetStreamSerializer<int>(out var keySerializer);
            keySerializer.Serialize(123, classStreamMessageSerialized.KeyStream, default);
            naiveSerializer.Serialize(new DummyClass(), classStreamMessageSerialized.ValueStream, default);

        }

        [GlobalCleanup]
        public void Cleanup()
        {
            producerByteArray.Dispose();
            streamProducerByteArray.Dispose();
            producerString.Dispose();
            streamProducerString.Dispose();
            producerDouble.Dispose();
            streamProducerDouble.Dispose();
            producerClass.Dispose();
            streamProducerClass.Dispose();
        }

        // Re-uses the message
        [Benchmark]
        public void ByteArrayOld()
        {
            producerByteArray.Produce(topicPartition, byteMessage);
        }

        // Re-uses the message
        [Benchmark]
        public void StringOld()
        {
            producerString.Produce(topicPartition, stringMessage);
        }

        // Re-uses the message
        [Benchmark]
        public void DoubleOld()
        {
            producerDouble.Produce(topicPartition, doubleMessage);
        }

        // Re-uses the message
        [Benchmark]
        public void ClassOld()
        {
            producerClass.Produce(topicPartition, classMessage);
        }


        // Re-uses the message & streams
        [Benchmark]
        public void ByteArrayStream()
        {
            byteStreamMessage.KeyStream.Position = 0;
            byteStreamMessage.ValueStream.Position = 0;

            streamProducerByteArray.Produce(topicPartition, byteStreamMessage);

        }

        // Re-uses the message & streams
        [Benchmark]
        public void StringStream()
        {
            stringStreamMessage.KeyStream.Position = 0;
            stringStreamMessage.ValueStream.Position = 0;

            streamProducerString.Produce(topicPartition, stringStreamMessage);
        }

        // Re-uses the message & streams
        [Benchmark]
        public void DoubleStream()
        {

            doubleStreamMessage.KeyStream.Position = 0;
            doubleStreamMessage.ValueStream.Position = 0;

            streamProducerDouble.Produce(topicPartition, doubleStreamMessage);
        }

        // Re-uses the message & streams
        [Benchmark]
        public void ClassStream()
        {

            classStreamMessage.KeyStream.Position = 0;
            classStreamMessage.ValueStream.Position = 0;

            streamProducerClass.Produce(topicPartition, classStreamMessage);
        }

        [Benchmark]
        public void ClassStreamPreSerialized()
        {
            streamProducerClass.Produce(topicPartition, classStreamMessageSerialized);
        }
    }

    // Test class just to have a type to expect a serializer for
    public class DummyClass
    {

    }

    public class NaiveSerializer<T> : ISerializer<T>, IStreamSerializer<T>
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
    }
}
