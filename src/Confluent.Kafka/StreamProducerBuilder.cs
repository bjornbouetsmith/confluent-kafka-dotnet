using System;
using System.Collections.Generic;

namespace Confluent.Kafka
{
    public class StreamProducerBuilder<TKey,TValue> : ProducerBuilder<TKey, TValue>
    {
        public StreamProducerBuilder(IEnumerable<KeyValuePair<string, string>> config) : base(config)
        {
        }

        /// <summary>
        ///     The configured key serializer.
        /// </summary>
        protected internal IStreamSerializer<TKey> KeyStreamSerializer { get; set; }

        /// <summary>
        ///     The configured value serializer.
        /// </summary>
        protected internal IStreamSerializer<TValue> ValueStreamSerializer { get; set; }

        

        public virtual IStreamProducer<TKey,TValue> BuildStreamProducer()
        {
            return new StreamProducer<TKey,TValue>(this);
        }

        public StreamProducerBuilder<TKey, TValue> SetValueSerializer(IStreamSerializer<TValue> serializer)
        {
            if (this.ValueStreamSerializer != null)
            {
                throw new InvalidOperationException("Value serializer may not be specified more than once.");
            }

            this.ValueStreamSerializer = serializer;
            return this;
        }
        
        public StreamProducerBuilder<TKey, TValue> SetKeySerializer(IStreamSerializer<TKey> serializer)
        {
            if (this.KeyStreamSerializer != null)
            {
                throw new InvalidOperationException("Key serializer may not be specified more than once.");
            }

            this.KeyStreamSerializer = serializer;
            return this;
        }
    }
}