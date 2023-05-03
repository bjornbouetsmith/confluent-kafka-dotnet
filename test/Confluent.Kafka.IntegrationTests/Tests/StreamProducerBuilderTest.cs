using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void CreateStreamProducer(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            using var producer = new StreamProducerBuilder<string, string>(producerConfig).BuildStreamProducer();
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public void CreateStreamProducerWithCustomSerializer(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            var builder = new StreamProducerBuilder<string, string>(producerConfig);
            builder.SetKeySerializer(new StringStreamSerializer());
            builder.SetValueSerializer(new StringStreamSerializer());
            using var producer = builder.BuildStreamProducer();
        }

        private class StringStreamSerializer : IStreamSerializer<string>
        {
            public void Serialize(string data, MemoryStream targetStream, SerializationContext context)
            {
                using var sw = new StreamWriter(targetStream, Encoding.UTF8,-1,true);
                sw.Write(data);
            }

            public async Task SerializeAsync(string data, MemoryStream targetStream, SerializationContext context, CancellationToken cancellationToken = default)
            {
                await using var sw = new StreamWriter(targetStream, Encoding.UTF8, -1, true);
                await sw.WriteAsync(data);
            }
        }
    }
}
