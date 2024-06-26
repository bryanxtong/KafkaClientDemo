// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: SimpleMessage.proto

package org.example;

public final class SimpleMessageProto {
  private SimpleMessageProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface SimpleMessageOrBuilder extends
      // @@protoc_insertion_point(interface_extends:org.example.SimpleMessage)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>string content = 1;</code>
     * @return The content.
     */
    java.lang.String getContent();
    /**
     * <code>string content = 1;</code>
     * @return The bytes for content.
     */
    com.google.protobuf.ByteString
        getContentBytes();

    /**
     * <code>string date_time = 2;</code>
     * @return The dateTime.
     */
    java.lang.String getDateTime();
    /**
     * <code>string date_time = 2;</code>
     * @return The bytes for dateTime.
     */
    com.google.protobuf.ByteString
        getDateTimeBytes();
  }
  /**
   * Protobuf type {@code org.example.SimpleMessage}
   */
  public  static final class SimpleMessage extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:org.example.SimpleMessage)
      SimpleMessageOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use SimpleMessage.newBuilder() to construct.
    private SimpleMessage(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private SimpleMessage() {
      content_ = "";
      dateTime_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new SimpleMessage();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private SimpleMessage(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 10: {
              java.lang.String s = input.readStringRequireUtf8();

              content_ = s;
              break;
            }
            case 18: {
              java.lang.String s = input.readStringRequireUtf8();

              dateTime_ = s;
              break;
            }
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.example.SimpleMessageProto.internal_static_org_example_SimpleMessage_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.example.SimpleMessageProto.internal_static_org_example_SimpleMessage_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.example.SimpleMessageProto.SimpleMessage.class, org.example.SimpleMessageProto.SimpleMessage.Builder.class);
    }

    public static final int CONTENT_FIELD_NUMBER = 1;
    private volatile java.lang.Object content_;
    /**
     * <code>string content = 1;</code>
     * @return The content.
     */
    public java.lang.String getContent() {
      java.lang.Object ref = content_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        content_ = s;
        return s;
      }
    }
    /**
     * <code>string content = 1;</code>
     * @return The bytes for content.
     */
    public com.google.protobuf.ByteString
        getContentBytes() {
      java.lang.Object ref = content_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        content_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int DATE_TIME_FIELD_NUMBER = 2;
    private volatile java.lang.Object dateTime_;
    /**
     * <code>string date_time = 2;</code>
     * @return The dateTime.
     */
    public java.lang.String getDateTime() {
      java.lang.Object ref = dateTime_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        dateTime_ = s;
        return s;
      }
    }
    /**
     * <code>string date_time = 2;</code>
     * @return The bytes for dateTime.
     */
    public com.google.protobuf.ByteString
        getDateTimeBytes() {
      java.lang.Object ref = dateTime_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        dateTime_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (!getContentBytes().isEmpty()) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 1, content_);
      }
      if (!getDateTimeBytes().isEmpty()) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 2, dateTime_);
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (!getContentBytes().isEmpty()) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, content_);
      }
      if (!getDateTimeBytes().isEmpty()) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, dateTime_);
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof org.example.SimpleMessageProto.SimpleMessage)) {
        return super.equals(obj);
      }
      org.example.SimpleMessageProto.SimpleMessage other = (org.example.SimpleMessageProto.SimpleMessage) obj;

      if (!getContent()
          .equals(other.getContent())) return false;
      if (!getDateTime()
          .equals(other.getDateTime())) return false;
      if (!unknownFields.equals(other.unknownFields)) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      hash = (37 * hash) + CONTENT_FIELD_NUMBER;
      hash = (53 * hash) + getContent().hashCode();
      hash = (37 * hash) + DATE_TIME_FIELD_NUMBER;
      hash = (53 * hash) + getDateTime().hashCode();
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.example.SimpleMessageProto.SimpleMessage parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.example.SimpleMessageProto.SimpleMessage parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.example.SimpleMessageProto.SimpleMessage parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.example.SimpleMessageProto.SimpleMessage parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.example.SimpleMessageProto.SimpleMessage parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.example.SimpleMessageProto.SimpleMessage parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.example.SimpleMessageProto.SimpleMessage parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.example.SimpleMessageProto.SimpleMessage parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.example.SimpleMessageProto.SimpleMessage parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.example.SimpleMessageProto.SimpleMessage parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.example.SimpleMessageProto.SimpleMessage parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.example.SimpleMessageProto.SimpleMessage parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(org.example.SimpleMessageProto.SimpleMessage prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code org.example.SimpleMessage}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:org.example.SimpleMessage)
        org.example.SimpleMessageProto.SimpleMessageOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.example.SimpleMessageProto.internal_static_org_example_SimpleMessage_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.example.SimpleMessageProto.internal_static_org_example_SimpleMessage_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.example.SimpleMessageProto.SimpleMessage.class, org.example.SimpleMessageProto.SimpleMessage.Builder.class);
      }

      // Construct using org.example.SimpleMessageProto.SimpleMessage.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        content_ = "";

        dateTime_ = "";

        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.example.SimpleMessageProto.internal_static_org_example_SimpleMessage_descriptor;
      }

      @java.lang.Override
      public org.example.SimpleMessageProto.SimpleMessage getDefaultInstanceForType() {
        return org.example.SimpleMessageProto.SimpleMessage.getDefaultInstance();
      }

      @java.lang.Override
      public org.example.SimpleMessageProto.SimpleMessage build() {
        org.example.SimpleMessageProto.SimpleMessage result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.example.SimpleMessageProto.SimpleMessage buildPartial() {
        org.example.SimpleMessageProto.SimpleMessage result = new org.example.SimpleMessageProto.SimpleMessage(this);
        result.content_ = content_;
        result.dateTime_ = dateTime_;
        onBuilt();
        return result;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.example.SimpleMessageProto.SimpleMessage) {
          return mergeFrom((org.example.SimpleMessageProto.SimpleMessage)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.example.SimpleMessageProto.SimpleMessage other) {
        if (other == org.example.SimpleMessageProto.SimpleMessage.getDefaultInstance()) return this;
        if (!other.getContent().isEmpty()) {
          content_ = other.content_;
          onChanged();
        }
        if (!other.getDateTime().isEmpty()) {
          dateTime_ = other.dateTime_;
          onChanged();
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.example.SimpleMessageProto.SimpleMessage parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.example.SimpleMessageProto.SimpleMessage) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }

      private java.lang.Object content_ = "";
      /**
       * <code>string content = 1;</code>
       * @return The content.
       */
      public java.lang.String getContent() {
        java.lang.Object ref = content_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          content_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string content = 1;</code>
       * @return The bytes for content.
       */
      public com.google.protobuf.ByteString
          getContentBytes() {
        java.lang.Object ref = content_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          content_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>string content = 1;</code>
       * @param value The content to set.
       * @return This builder for chaining.
       */
      public Builder setContent(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  
        content_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>string content = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearContent() {
        
        content_ = getDefaultInstance().getContent();
        onChanged();
        return this;
      }
      /**
       * <code>string content = 1;</code>
       * @param value The bytes for content to set.
       * @return This builder for chaining.
       */
      public Builder setContentBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
        
        content_ = value;
        onChanged();
        return this;
      }

      private java.lang.Object dateTime_ = "";
      /**
       * <code>string date_time = 2;</code>
       * @return The dateTime.
       */
      public java.lang.String getDateTime() {
        java.lang.Object ref = dateTime_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          dateTime_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string date_time = 2;</code>
       * @return The bytes for dateTime.
       */
      public com.google.protobuf.ByteString
          getDateTimeBytes() {
        java.lang.Object ref = dateTime_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          dateTime_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>string date_time = 2;</code>
       * @param value The dateTime to set.
       * @return This builder for chaining.
       */
      public Builder setDateTime(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  
        dateTime_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>string date_time = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearDateTime() {
        
        dateTime_ = getDefaultInstance().getDateTime();
        onChanged();
        return this;
      }
      /**
       * <code>string date_time = 2;</code>
       * @param value The bytes for dateTime to set.
       * @return This builder for chaining.
       */
      public Builder setDateTimeBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
        
        dateTime_ = value;
        onChanged();
        return this;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:org.example.SimpleMessage)
    }

    // @@protoc_insertion_point(class_scope:org.example.SimpleMessage)
    private static final org.example.SimpleMessageProto.SimpleMessage DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.example.SimpleMessageProto.SimpleMessage();
    }

    public static org.example.SimpleMessageProto.SimpleMessage getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<SimpleMessage>
        PARSER = new com.google.protobuf.AbstractParser<SimpleMessage>() {
      @java.lang.Override
      public SimpleMessage parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new SimpleMessage(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<SimpleMessage> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<SimpleMessage> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.example.SimpleMessageProto.SimpleMessage getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_org_example_SimpleMessage_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_org_example_SimpleMessage_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\023SimpleMessage.proto\022\013org.example\"3\n\rSi" +
      "mpleMessage\022\017\n\007content\030\001 \001(\t\022\021\n\tdate_tim" +
      "e\030\002 \001(\tB\024B\022SimpleMessageProtob\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_org_example_SimpleMessage_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_org_example_SimpleMessage_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_org_example_SimpleMessage_descriptor,
        new java.lang.String[] { "Content", "DateTime", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
