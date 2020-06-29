// <auto-generated>
//     Generated by the protocol buffer compiler.  DO NOT EDIT!
//     source: person.proto
// </auto-generated>
#pragma warning disable 1591, 0612, 3021
#region Designer generated code

using pb = global::Google.Protobuf;
using pbc = global::Google.Protobuf.Collections;
using pbr = global::Google.Protobuf.Reflection;
using scg = global::System.Collections.Generic;
namespace Confluent.Kafka.Examples.Protobuf {

  /// <summary>Holder for reflection information generated from person.proto</summary>
  public static partial class PersonReflection {

    #region Descriptor
    /// <summary>File descriptor for person.proto</summary>
    public static pbr::FileDescriptor Descriptor {
      get { return descriptor; }
    }
    private static pbr::FileDescriptor descriptor;

    static PersonReflection() {
      byte[] descriptorData = global::System.Convert.FromBase64String(
          string.Concat(
            "CgxwZXJzb24ucHJvdG8SIWNvbmZsdWVudC5rYWZrYS5leGFtcGxlcy5wcm90",
            "b2J1ZhoRcGVyc29uX25hbWUucHJvdG8iUgoGUGVyc29uEjsKBE5hbWUYASAB",
            "KAsyLS5jb25mbHVlbnQua2Fma2EuZXhhbXBsZXMucHJvdG9idWYuUGVyc29u",
            "TmFtZRILCgNBZ2UYAiABKAViBnByb3RvMw=="));
      descriptor = pbr::FileDescriptor.FromGeneratedCode(descriptorData,
          new pbr::FileDescriptor[] { global::Confluent.Kafka.Examples.Protobuf.PersonNameReflection.Descriptor, },
          new pbr::GeneratedClrTypeInfo(null, new pbr::GeneratedClrTypeInfo[] {
            new pbr::GeneratedClrTypeInfo(typeof(global::Confluent.Kafka.Examples.Protobuf.Person), global::Confluent.Kafka.Examples.Protobuf.Person.Parser, new[]{ "Name", "Age" }, null, null, null)
          }));
    }
    #endregion

  }
  #region Messages
  public sealed partial class Person : pb::IMessage<Person> {
    private static readonly pb::MessageParser<Person> _parser = new pb::MessageParser<Person>(() => new Person());
    private pb::UnknownFieldSet _unknownFields;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<Person> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Confluent.Kafka.Examples.Protobuf.PersonReflection.Descriptor.MessageTypes[0]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Person() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Person(Person other) : this() {
      name_ = other.name_ != null ? other.name_.Clone() : null;
      age_ = other.age_;
      _unknownFields = pb::UnknownFieldSet.Clone(other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Person Clone() {
      return new Person(this);
    }

    /// <summary>Field number for the "Name" field.</summary>
    public const int NameFieldNumber = 1;
    private global::Confluent.Kafka.Examples.Protobuf.PersonName name_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public global::Confluent.Kafka.Examples.Protobuf.PersonName Name {
      get { return name_; }
      set {
        name_ = value;
      }
    }

    /// <summary>Field number for the "Age" field.</summary>
    public const int AgeFieldNumber = 2;
    private int age_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int Age {
      get { return age_; }
      set {
        age_ = value;
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as Person);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(Person other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (!object.Equals(Name, other.Name)) return false;
      if (Age != other.Age) return false;
      return Equals(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      if (name_ != null) hash ^= Name.GetHashCode();
      if (Age != 0) hash ^= Age.GetHashCode();
      if (_unknownFields != null) {
        hash ^= _unknownFields.GetHashCode();
      }
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void WriteTo(pb::CodedOutputStream output) {
      if (name_ != null) {
        output.WriteRawTag(10);
        output.WriteMessage(Name);
      }
      if (Age != 0) {
        output.WriteRawTag(16);
        output.WriteInt32(Age);
      }
      if (_unknownFields != null) {
        _unknownFields.WriteTo(output);
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      if (name_ != null) {
        size += 1 + pb::CodedOutputStream.ComputeMessageSize(Name);
      }
      if (Age != 0) {
        size += 1 + pb::CodedOutputStream.ComputeInt32Size(Age);
      }
      if (_unknownFields != null) {
        size += _unknownFields.CalculateSize();
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(Person other) {
      if (other == null) {
        return;
      }
      if (other.name_ != null) {
        if (name_ == null) {
          name_ = new global::Confluent.Kafka.Examples.Protobuf.PersonName();
        }
        Name.MergeFrom(other.Name);
      }
      if (other.Age != 0) {
        Age = other.Age;
      }
      _unknownFields = pb::UnknownFieldSet.MergeFrom(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(pb::CodedInputStream input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, input);
            break;
          case 10: {
            if (name_ == null) {
              name_ = new global::Confluent.Kafka.Examples.Protobuf.PersonName();
            }
            input.ReadMessage(name_);
            break;
          }
          case 16: {
            Age = input.ReadInt32();
            break;
          }
        }
      }
    }

  }

  #endregion

}

#endregion Designer generated code