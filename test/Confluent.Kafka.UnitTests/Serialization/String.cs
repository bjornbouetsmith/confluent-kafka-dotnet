// Copyright 2016-2017 Confluent Inc.
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

using Xunit;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System;
using System.IO;


namespace Confluent.Kafka.UnitTests.Serialization
{
    public class StringTests
    {
        public static IEnumerable<object[]> StringData
        {
            get
            {
                yield return new object[] { "hello world" };
                yield return new object[] { "ឆ្មាត្រូវបានហែលទឹក" };
                yield return new object[] { "вы не банан" };
            }
        }

        [Theory]
        [MemberData(nameof(StringData))]
        public void SerializeDeserialize(string value)
        {
            Assert.Equal(value, Deserializers.Utf8.Deserialize(Serializers.Utf8.Serialize(value, SerializationContext.Empty), false, SerializationContext.Empty));
            Assert.Null(Deserializers.Utf8.Deserialize(Serializers.Utf8.Serialize(null, SerializationContext.Empty), true, SerializationContext.Empty));

            // TODO: check some serialize / deserialize operations that are not expected to work, including some
            //       cases where Deserialize can be expected to throw an exception.
        }

        [Theory]
        [MemberData(nameof(StringData))]
        public void CanSerializeToStreamAndReconstructString(string value)
        {
            var serializer = (IStreamSerializer<string>)Serializers.Utf8;
            var stream = new MemoryStream();
            serializer.Serialize(value, stream, SerializationContext.Empty);
            var span = new ReadOnlySpan<byte>(stream.ToArray(), 0, (int)stream.Position);
            Assert.Equal(value, Deserializers.Utf8.Deserialize(span, false, SerializationContext.Empty));
        }

    }
}
