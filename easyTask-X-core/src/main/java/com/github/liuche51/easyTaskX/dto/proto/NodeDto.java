package com.github.liuche51.easyTaskX.dto.proto;// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: NodeDto.proto

public final class NodeDto {
  private NodeDto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface NodeOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required string host = 1;
    /**
     * <code>required string host = 1;</code>
     */
    boolean hasHost();
    /**
     * <code>required string host = 1;</code>
     */
    String getHost();
    /**
     * <code>required string host = 1;</code>
     */
    com.google.protobuf.ByteString
        getHostBytes();

    // required int32 port = 2;
    /**
     * <code>required int32 port = 2;</code>
     */
    boolean hasPort();
    /**
     * <code>required int32 port = 2;</code>
     */
    int getPort();

    // optional int32 dataStatus = 3;
    /**
     * <code>optional int32 dataStatus = 3;</code>
     */
    boolean hasDataStatus();
    /**
     * <code>optional int32 dataStatus = 3;</code>
     */
    int getDataStatus();
  }
  /**
   * Protobuf type {@code Node}
   */
  public static final class Node extends
      com.google.protobuf.GeneratedMessage
      implements NodeOrBuilder {
    // Use Node.newBuilder() to construct.
    private Node(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private Node(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final Node defaultInstance;
    public static Node getDefaultInstance() {
      return defaultInstance;
    }

    public Node getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private Node(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
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
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              host_ = input.readBytes();
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              port_ = input.readInt32();
              break;
            }
            case 24: {
              bitField0_ |= 0x00000004;
              dataStatus_ = input.readInt32();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return NodeDto.internal_static_Node_descriptor;
    }

    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return NodeDto.internal_static_Node_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              Node.class, Builder.class);
    }

    public static com.google.protobuf.Parser<Node> PARSER =
        new com.google.protobuf.AbstractParser<Node>() {
      public Node parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new Node(input, extensionRegistry);
      }
    };

    @Override
    public com.google.protobuf.Parser<Node> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required string host = 1;
    public static final int HOST_FIELD_NUMBER = 1;
    private Object host_;
    /**
     * <code>required string host = 1;</code>
     */
    public boolean hasHost() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required string host = 1;</code>
     */
    public String getHost() {
      Object ref = host_;
      if (ref instanceof String) {
        return (String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          host_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string host = 1;</code>
     */
    public com.google.protobuf.ByteString
        getHostBytes() {
      Object ref = host_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        host_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // required int32 port = 2;
    public static final int PORT_FIELD_NUMBER = 2;
    private int port_;
    /**
     * <code>required int32 port = 2;</code>
     */
    public boolean hasPort() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required int32 port = 2;</code>
     */
    public int getPort() {
      return port_;
    }

    // optional int32 dataStatus = 3;
    public static final int DATASTATUS_FIELD_NUMBER = 3;
    private int dataStatus_;
    /**
     * <code>optional int32 dataStatus = 3;</code>
     */
    public boolean hasDataStatus() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional int32 dataStatus = 3;</code>
     */
    public int getDataStatus() {
      return dataStatus_;
    }

    private void initFields() {
      host_ = "";
      port_ = 0;
      dataStatus_ = 0;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasHost()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasPort()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, getHostBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeInt32(2, port_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeInt32(3, dataStatus_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, getHostBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(2, port_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(3, dataStatus_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @Override
    protected Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static Node parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static Node parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static Node parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static Node parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static Node parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static Node parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static Node parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static Node parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static Node parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static Node parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(Node prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @Override
    protected Builder newBuilderForType(
        BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code Node}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements NodeOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return NodeDto.internal_static_Node_descriptor;
      }

      protected FieldAccessorTable
          internalGetFieldAccessorTable() {
        return NodeDto.internal_static_Node_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                Node.class, Builder.class);
      }

      // Construct using NodeDto.Node.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        host_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        port_ = 0;
        bitField0_ = (bitField0_ & ~0x00000002);
        dataStatus_ = 0;
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return NodeDto.internal_static_Node_descriptor;
      }

      public Node getDefaultInstanceForType() {
        return Node.getDefaultInstance();
      }

      public Node build() {
        Node result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public Node buildPartial() {
        Node result = new Node(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.host_ = host_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.port_ = port_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.dataStatus_ = dataStatus_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof Node) {
          return mergeFrom((Node)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Node other) {
        if (other == Node.getDefaultInstance()) return this;
        if (other.hasHost()) {
          bitField0_ |= 0x00000001;
          host_ = other.host_;
          onChanged();
        }
        if (other.hasPort()) {
          setPort(other.getPort());
        }
        if (other.hasDataStatus()) {
          setDataStatus(other.getDataStatus());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasHost()) {
          
          return false;
        }
        if (!hasPort()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        Node parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (Node) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required string host = 1;
      private Object host_ = "";
      /**
       * <code>required string host = 1;</code>
       */
      public boolean hasHost() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required string host = 1;</code>
       */
      public String getHost() {
        Object ref = host_;
        if (!(ref instanceof String)) {
          String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          host_ = s;
          return s;
        } else {
          return (String) ref;
        }
      }
      /**
       * <code>required string host = 1;</code>
       */
      public com.google.protobuf.ByteString
          getHostBytes() {
        Object ref = host_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (String) ref);
          host_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string host = 1;</code>
       */
      public Builder setHost(
          String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        host_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string host = 1;</code>
       */
      public Builder clearHost() {
        bitField0_ = (bitField0_ & ~0x00000001);
        host_ = getDefaultInstance().getHost();
        onChanged();
        return this;
      }
      /**
       * <code>required string host = 1;</code>
       */
      public Builder setHostBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        host_ = value;
        onChanged();
        return this;
      }

      // required int32 port = 2;
      private int port_ ;
      /**
       * <code>required int32 port = 2;</code>
       */
      public boolean hasPort() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required int32 port = 2;</code>
       */
      public int getPort() {
        return port_;
      }
      /**
       * <code>required int32 port = 2;</code>
       */
      public Builder setPort(int value) {
        bitField0_ |= 0x00000002;
        port_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required int32 port = 2;</code>
       */
      public Builder clearPort() {
        bitField0_ = (bitField0_ & ~0x00000002);
        port_ = 0;
        onChanged();
        return this;
      }

      // optional int32 dataStatus = 3;
      private int dataStatus_ ;
      /**
       * <code>optional int32 dataStatus = 3;</code>
       */
      public boolean hasDataStatus() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional int32 dataStatus = 3;</code>
       */
      public int getDataStatus() {
        return dataStatus_;
      }
      /**
       * <code>optional int32 dataStatus = 3;</code>
       */
      public Builder setDataStatus(int value) {
        bitField0_ |= 0x00000004;
        dataStatus_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 dataStatus = 3;</code>
       */
      public Builder clearDataStatus() {
        bitField0_ = (bitField0_ & ~0x00000004);
        dataStatus_ = 0;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:Node)
    }

    static {
      defaultInstance = new Node(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:Node)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_Node_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_Node_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    String[] descriptorData = {
      "\n\rNodeDto.proto\"6\n\004Node\022\014\n\004host\030\001 \002(\t\022\014\n" +
      "\004port\030\002 \002(\005\022\022\n\ndataStatus\030\003 \001(\005B\tB\007NodeD" +
      "to"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_Node_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_Node_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_Node_descriptor,
              new String[] { "Host", "Port", "DataStatus", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}