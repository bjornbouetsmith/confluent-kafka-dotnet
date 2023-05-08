using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.StreamBenchmark
{
    //[SimpleJob(RuntimeMoniker.Net462, baseline: true)]
    [SimpleJob(RuntimeMoniker.Net60)]
    [SimpleJob(RuntimeMoniker.Net48)]
    //[SimpleJob(RuntimeMoniker.NativeAot70)]
    [RPlotExporter]
    [MemoryDiagnoser]
    public class StreamVsOld
    {
        private byte[] data;
        private string stringValue;
        private IProducer<int, byte[]> streamProducerByteArray;
        private IProducer<int, byte[]> producerByteArray;
        private string topic;
        private IProducer<int, string> streamProducerString;
        private IProducer<int, string> producerString;
        private IProducer<int, double> producerDouble;
        private IProducer<int, DummyClass> producerClass;

        private Message<int, byte[]> byteMessage;
        private Message<int, string> stringMessage;
        private Message<int, double> doubleMessage;
        private Message<int, DummyClass> classMessage;

     
        private IProducer<int, double> streamProducerDouble;
        private IProducer<int, DummyClass> streamProducerClass;
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
            streamProducerByteArray = new ProducerBuilder<int, byte[]>(producerConfig).SetValueSerializer(new KeySerializer()).SetKeySerializer(new KeySerializer()).Build();
            producerByteArray = new ProducerBuilder<int, byte[]>(producerConfig).Build();
            streamProducerString = new ProducerBuilder<int, string>(producerConfig).SetValueSerializer(new KeySerializer()).SetKeySerializer(new KeySerializer()).Build();
            streamProducerDouble = new ProducerBuilder<int, double>(producerConfig).SetValueSerializer(new KeySerializer()).SetKeySerializer(new KeySerializer()).Build();
            var naiveSerializer = new NaiveSerializer<DummyClass>(data);
            streamProducerClass = new ProducerBuilder<int, DummyClass>(producerConfig).SetValueSerializer((IMemorySerializer<DummyClass>)naiveSerializer).SetKeySerializer(new KeySerializer()).Build();
            producerString = new ProducerBuilder<int, string>(producerConfig).Build();
            producerDouble = new ProducerBuilder<int, double>(producerConfig).Build();
            producerClass = new ProducerBuilder<int, DummyClass>(producerConfig).SetValueSerializer((ISerializer<DummyClass>)naiveSerializer).Build();

            byteMessage = new Message<int, byte[]> { Headers = new Headers(), Key = 123, Value = data };
            stringMessage = new Message<int, string> { Headers = new Headers(), Key = 123, Value = stringValue };
            doubleMessage = new Message<int, double> { Headers = new Headers(), Key = 123, Value = double.MaxValue / 2 - 1.23456789d };
            classMessage = new Message<int, DummyClass> { Headers = new Headers(), Key = 123, Value = new DummyClass() };
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
            streamProducerByteArray.Produce(topicPartition, byteMessage);
        }

        // Re-uses the message & streams
        [Benchmark]
        public void StringStream()
        {
            streamProducerString.Produce(topicPartition, stringMessage);
        }

        // Re-uses the message & streams
        [Benchmark]
        public void DoubleStream()
        {
            streamProducerDouble.Produce(topicPartition, doubleMessage);
        }

        // Re-uses the message & streams
        [Benchmark]
        public void ClassStream()
        {

            streamProducerClass.Produce(topicPartition, classMessage);
        }
    }

    // Test class just to have a type to expect a serializer for
    public class DummyClass
    {

    }
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
    public class KeySerializer : IMemorySerializer<string>, IMemorySerializer<long>, IMemorySerializer<int>, IMemorySerializer<Null>, IMemorySerializer<float>, IMemorySerializer<double>, IMemorySerializer<byte[]>
    {
        [ThreadStatic]
        private static MemoryStream _ms;
        public ReadOnlyMemory<byte> Serialize(string data, SerializationContext context)
        {
            if (data == null)
            {
                return default;
            }

            data.AsMemory();
            var targetStream = _ms ??= new MemoryStream();
            targetStream.Position = 0;
            // Set length to worst case size
            targetStream.SetLength(data.Length * 4);

            var buffer = targetStream.GetBufferAsArraySegment();

            // This should be the most efficient way to convert a string and write to destination byte array, 
            // since UTFEncoding does not allocate memory
            unsafe
            {
                fixed (byte* bytes = buffer.Array)
                fixed (char* chars = data)
                {
                    var length = Encoding.UTF8.GetByteCount(chars, data.Length);
                    targetStream.SetLength(length);
                    length = Encoding.UTF8.GetBytes(chars, data.Length, bytes, length);
                    targetStream.Position += length;
                }
            }

            return new ReadOnlyMemory<byte>(buffer.Array, 0, (int)targetStream.Position);
        }

        public Task<ReadOnlyMemory<byte>> SerializeAsync(string data, SerializationContext context, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(Serialize(data, context));
        }

        public ReadOnlyMemory<byte> Serialize(long data, SerializationContext context)
        {
            var targetStream = _ms ??= new MemoryStream();
            targetStream.Position = 0;
            targetStream.SetLength(8);
            var buffer = targetStream.GetBufferAsArraySegment();
            

            SerializeToByteArray(data, buffer.Array, buffer.Offset);
            targetStream.Position += 8;
            return new ReadOnlyMemory<byte>(buffer.Array, 0, (int)targetStream.Position);
        }
        private static void SerializeToByteArray(long data, byte[] result, int offset)
        {
            result[offset + 0] = (byte)(data >> 56);
            result[offset + 1] = (byte)(data >> 48);
            result[offset + 2] = (byte)(data >> 40);
            result[offset + 3] = (byte)(data >> 32);
            result[offset + 4] = (byte)(data >> 24);
            result[offset + 5] = (byte)(data >> 16);
            result[offset + 6] = (byte)(data >> 8);
            result[offset + 7] = (byte)data;
        }

        public Task<ReadOnlyMemory<byte>> SerializeAsync(long data, SerializationContext context, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(Serialize(data, context));
        }

        public ReadOnlyMemory<byte> Serialize(int data, SerializationContext context)
        {
            var targetStream = _ms ??= new MemoryStream();
            targetStream.Position = 0;
            targetStream.SetLength(4);
            var buffer = targetStream.GetBufferAsArraySegment();
            

            SerializeToByteArray(data, buffer.Array, buffer.Offset);
            targetStream.Position += 4;
            return new ReadOnlyMemory<byte>(buffer.Array, 0, (int)targetStream.Position);
        }
        private static void SerializeToByteArray(int data, byte[] result, int offset)
        {
            // network byte order -> big endian -> most significant byte in the smallest address.
            // Note: At the IL level, the conv.u1 operator is used to cast int to byte which truncates
            // the high order bits if overflow occurs.
            // https://msdn.microsoft.com/en-us/library/system.reflection.emit.opcodes.conv_u1.aspx
            result[offset + 0] = (byte)(data >> 24);
            result[offset + 1] = (byte)(data >> 16); // & 0xff;
            result[offset + 2] = (byte)(data >> 8); // & 0xff;
            result[offset + 3] = (byte)data; // & 0xff;
        }

        public Task<ReadOnlyMemory<byte>> SerializeAsync(int data, SerializationContext context, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(Serialize(data, context));
        }

        public ReadOnlyMemory<byte> Serialize(Null data, SerializationContext context)
        {
            return default;
        }

        public Task<ReadOnlyMemory<byte>> SerializeAsync(Null data, SerializationContext context, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(Serialize(data, context));
        }

        public ReadOnlyMemory<byte> Serialize(float data, SerializationContext context)
        {
            if (BitConverter.IsLittleEndian)
            {
                var targetStream = _ms ??= new MemoryStream();
                targetStream.Position = 0;
                targetStream.SetLength(4);
                var buffer = targetStream.GetBufferAsArraySegment();
                SerializeLittleEndian(data, buffer.Array, buffer.Offset);
                targetStream.Position += 4;
                return new ReadOnlyMemory<byte>(buffer.Array, 0, (int)targetStream.Position);
            }

            var bytes = BitConverter.GetBytes(data);

            return new ReadOnlyMemory<byte>(bytes, 0, bytes.Length);
        }
        private static void SerializeLittleEndian(float data, byte[] result, int offset)
        {
            unsafe
            {
                byte* p = (byte*)(&data);
                result[offset + 3] = *p++;
                result[offset + 2] = *p++;
                result[offset + 1] = *p++;
                result[offset + 0] = *p++;
            }
        }

        public Task<ReadOnlyMemory<byte>> SerializeAsync(float data, SerializationContext context, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(Serialize(data, context));
        }

        public ReadOnlyMemory<byte> Serialize(double data, SerializationContext context)
        {
            if (BitConverter.IsLittleEndian)
            {
                var targetStream = _ms ??= new MemoryStream();
                targetStream.Position = 0;
                targetStream.SetLength(8);
                var buffer = targetStream.GetBufferAsArraySegment();
                SerializeLittleEndian(data, buffer.Array, buffer.Offset);
                targetStream.Position += 8;
                return new ReadOnlyMemory<byte>(buffer.Array, 0, (int)targetStream.Position);
            }

            var bytes = BitConverter.GetBytes(data);

            return new ReadOnlyMemory<byte>(bytes, 0, bytes.Length);
        }
        private static void SerializeLittleEndian(double data, byte[] result, int offset)
        {
            unsafe
            {
                byte* p = (byte*)(&data);

                result[offset + 7] = *p++;
                result[offset + 6] = *p++;
                result[offset + 5] = *p++;
                result[offset + 4] = *p++;
                result[offset + 3] = *p++;
                result[offset + 2] = *p++;
                result[offset + 1] = *p++;
                result[offset + 0] = *p++;
            }
        }

        public Task<ReadOnlyMemory<byte>> SerializeAsync(double data, SerializationContext context, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(Serialize(data, context));
        }

        public ReadOnlyMemory<byte> Serialize(byte[] data, SerializationContext context)
        {
            return data;
        }

        public Task<ReadOnlyMemory<byte>> SerializeAsync(byte[] data, SerializationContext context, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(Serialize(data, context));
        }
    }

    public class NaiveSerializer<T> : ISerializer<T>, IMemorySerializer<T>
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

        public Task<ReadOnlyMemory<byte>> SerializeAsync(T data, SerializationContext context, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(new ReadOnlyMemory<byte>(bytes));
        }

        ReadOnlyMemory<byte> IMemorySerializer<T>.Serialize(T data, SerializationContext context)
        {
            return new ReadOnlyMemory<byte>(bytes);
        }
    }
}
