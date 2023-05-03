// Copyright 2018 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Internal.Extensions;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Serializers for use with <see cref="Producer{TKey,TValue}" />.
    /// </summary>
    public static class Serializers
    {
        /// <summary>
        ///     String (UTF8) serializer.
        /// </summary>
        public static ISerializer<string> Utf8 = new Utf8Serializer();

        private static readonly Dictionary<Type, object> defaultSerializers;

        static Serializers()
        {
            defaultSerializers = new Dictionary<Type, object>
            {
                { typeof(Null), Null },
                { typeof(int), Int32 },
                { typeof(long), Int64 },
                { typeof(string), Utf8 },
                { typeof(float), Single },
                { typeof(double), Double },
                { typeof(byte[]), ByteArray }
            };
        }

        /// <summary>
        /// Tries to get a serializer from the list of default serializers registered
        /// </summary>
        public static bool TryGetSerializer<T>(out ISerializer<T> serializer)
        {
            if (defaultSerializers.TryGetValue(typeof(T), out object serializerObject))
            {
                serializer=(ISerializer<T>)serializerObject;
                return true;
            }

            serializer = null;
            return false;
        }

        /// <summary>
        /// Tries to get a stream serializer from the list of default serializers registered
        /// </summary>
        public static bool TryGetStreamSerializer<T>(out IStreamSerializer<T> serializer)
        {
            if (defaultSerializers.TryGetValue(typeof(T), out object serializerObject))
            {
                serializer = (IStreamSerializer<T>)serializerObject;
                return true;
            }

            serializer = null;
            return false;
        }


        private class Utf8Serializer : ISerializer<string>, IStreamSerializer<string>
        {
            public byte[] Serialize(string data, SerializationContext context)
            {
                if (data == null)
                {
                    return null;
                }

                return Encoding.UTF8.GetBytes(data);
            }

            public void Serialize(string data, MemoryStream targetStream, SerializationContext context)
            {
                if (data == null)
                {
                    return;
                }

                data.AsMemory();
                
                // Set length to worst case size
                targetStream.SetLength(data.Length * 4);

                var buffer = targetStream.GetBufferAsArraySegment();

                // This should be the most efficient way to convert a string and write to destination byte array, 
                // since UTFEncoding does not allocate memory
                unsafe
                {
                    fixed(byte* bytes = buffer.Array)
                    fixed (char* chars = data)
                    {
                        var length = Encoding.UTF8.GetByteCount(chars, data.Length);
                        targetStream.SetLength(length);
                        length = Encoding.UTF8.GetBytes(chars, data.Length, bytes, length);
                        targetStream.Position += length;
                    }
                }

            
            }

            public Task SerializeAsync(string data, MemoryStream targetStream, SerializationContext context, CancellationToken cancellationToken = default)
            {
                Serialize(data, targetStream, context);
                return Task.CompletedTask;
            }
        }


        /// <summary>
        ///     Null serializer.
        /// </summary>
        public static ISerializer<Null> Null = new NullSerializer();

        private class NullSerializer : ISerializer<Null>, IStreamSerializer<long>
        {
            public byte[] Serialize(Null data, SerializationContext context)
                => null;

            public void Serialize(long data, MemoryStream targetStream, SerializationContext context)
            {
                // Nothing to do here
            }

            public Task SerializeAsync(long data, MemoryStream targetStream, SerializationContext context, CancellationToken cancellationToken = default)
            {
                return Task.CompletedTask;
            }
        }


        /// <summary>
        ///     System.Int64 (big endian, network byte order) serializer.
        /// </summary>
        public static ISerializer<long> Int64 = new Int64Serializer();

        private class Int64Serializer : ISerializer<long>, IStreamSerializer<long>
        {
            public byte[] Serialize(long data, SerializationContext context)
            {
                var result = new byte[8];
                SerializeToByteArray(data, result, 0);
                return result;
            }

            public void Serialize(long data, MemoryStream targetStream, SerializationContext context)
            {
                targetStream.SetLength(8);
                var buffer = targetStream.GetBufferAsArraySegment();
                SerializeToByteArray(data, buffer.Array, buffer.Offset);
                targetStream.Position += 8;
            }

            public Task SerializeAsync(long data, MemoryStream targetStream, SerializationContext context, CancellationToken cancellationToken = default)
            {
                Serialize(data, targetStream, context);
                return Task.CompletedTask;
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
        }

        /// <summary>
        ///     System.Int32 (big endian, network byte order) serializer.
        /// </summary>
        public static ISerializer<int> Int32 = new Int32Serializer();

        private class Int32Serializer : ISerializer<int>, IStreamSerializer<int>
        {
            public byte[] Serialize(int data, SerializationContext context)
            {
                var result = new byte[4]; // int is always 32 bits on .NET.
                SerializeToByteArray(data, result, 0);
                return result;
            }

            public void Serialize(int data, MemoryStream targetStream, SerializationContext context)
            {
                targetStream.SetLength(4);
                var buffer = targetStream.GetBufferAsArraySegment();
                SerializeToByteArray(data, buffer.Array, buffer.Offset);
                targetStream.Position += 4;
            }

            public Task SerializeAsync(int data, MemoryStream targetStream, SerializationContext context, CancellationToken cancellationToken = default)
            {
                Serialize(data, targetStream, context);
                return Task.CompletedTask;
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
        }


        /// <summary>
        ///     System.Single (big endian, network byte order) serializer
        /// </summary>
        public static ISerializer<float> Single = new SingleSerializer();

        private class SingleSerializer : ISerializer<float>, IStreamSerializer<float>
        {
            public byte[] Serialize(float data, SerializationContext context)
            {
                if (BitConverter.IsLittleEndian)
                {
                    byte[] result = new byte[4];
                    SerializeLittleEndian(data, result, 0);
                    return result;
                }
                else
                {
                    return BitConverter.GetBytes(data);
                }
            }

            public void Serialize(float data, MemoryStream targetStream, SerializationContext context)
            {
                if (BitConverter.IsLittleEndian)
                {
                    targetStream.SetLength(4);
                    var buffer = targetStream.GetBufferAsArraySegment();
                    SerializeLittleEndian(data, buffer.Array, buffer.Offset);
                    targetStream.Position += 4;
                }
                else
                {
                    var bytes = BitConverter.GetBytes(data);
                    targetStream.Write(bytes, 0, bytes.Length);
                }
            }

            public Task SerializeAsync(float data, MemoryStream targetStream, SerializationContext context, CancellationToken cancellationToken = default)
            {
                Serialize(data, targetStream, context);
                return Task.CompletedTask;
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
        }


        /// <summary>
        ///     System.Double (big endian, network byte order) serializer
        /// </summary>
        public static ISerializer<double> Double = new DoubleSerializer();

        private class DoubleSerializer : ISerializer<double>, IStreamSerializer<double>
        {
            public byte[] Serialize(double data, SerializationContext context)
            {
                if (BitConverter.IsLittleEndian)
                {
                    byte[] result = new byte[8];
                    SerializeLittleEndian(data, result, 0);
                    return result;
                }
                else
                {
                    return BitConverter.GetBytes(data);
                }
            }

            public void Serialize(double data, MemoryStream targetStream, SerializationContext context)
            {
                targetStream.SetLength(8);
                if (BitConverter.IsLittleEndian)
                {
                    var buffer = targetStream.GetBufferAsArraySegment();
                    SerializeLittleEndian(data, buffer.Array, buffer.Offset);
                    targetStream.Position += 8;
                }
                else
                {
                    var bytes = BitConverter.GetBytes(data);
                    targetStream.Write(bytes, 0, bytes.Length);
                }
            }

            public Task SerializeAsync(double data, MemoryStream targetStream, SerializationContext context, CancellationToken cancellationToken = default)
            {
                Serialize(data, targetStream, context);

                return Task.CompletedTask;
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
        }


        /// <summary>
        ///     System.Byte[] (nullable) serializer.
        /// </summary>
        /// <remarks>
        ///     Byte order is original order.
        /// </remarks>
        public static ISerializer<byte[]> ByteArray = new ByteArraySerializer();

        private class ByteArraySerializer : ISerializer<byte[]>, IStreamSerializer<byte[]>
        {
            public byte[] Serialize(byte[] data, SerializationContext context)
                => data;

            public void Serialize(byte[] data, MemoryStream targetStream, SerializationContext context)
            {
                targetStream.Write(data, 0, data.Length);
            }

            public Task SerializeAsync(byte[] data, MemoryStream targetStream, SerializationContext context, CancellationToken cancellationToken = default)
            {
                Serialize(data, targetStream, context);
                
                return Task.CompletedTask;
            }
        }
    }
}
