import { createSocket, Socket } from 'dgram';
import { AddressInfo } from 'net';
import { EndPoint, UdpStream } from './UdpStream';
import { BufferSerializer, Serializer, Serializable, Oracle } from '@bhoos/serialization';

const GENERAL = 0;
const STREAM = 1;

export class UdpSocket<S = UdpStream> {
  private socket: Socket;
  private version: number;
  readonly oracle: Oracle;

  private handlers = new Map<number, (msg: Serializable) => void>();
  private streamHandlers = new Map<number, (msg: Serializable, userData: S, stream: UdpStream) => void>();
  private connectHandlers = new Map<number, (msg: Serializable, stream: UdpStream) => S>();

  private streams = new Map<string, { stream: UdpStream, userData: S}>();

  constructor(oracle: Oracle, version: number, port?: number) {
    this.version = version;
    this.oracle = oracle;
    this.socket = createSocket('udp4');
    if (port) {
      this.socket.bind(port);
    }

    this.socket.on('message', this.handleMessage);
  }

  close() {
    this.socket.off('message', this.handleMessage);
  }

  on<T extends Serializable>(clazz: new (...args:any[]) => T, handler: (msg: T) => void) {
    const id = this.oracle.id(clazz);
    this.handlers.set(id, handler);
  }

  onConnect<T extends Serializable>(clazz: new (...args: any[]) => T, handler: (msg: T, stream: UdpStream) => S) {
    const id = this.oracle.id(clazz);
    this.connectHandlers.set(id, handler);
  }

  onStream<T extends Serializable>(clazz: new (...args:any[]) => T, handler: (msg: T, stream: S) => void) {
    const id = this.oracle.id(clazz);
    this.streamHandlers.set(id, handler);
  }

  connect(endpoint: EndPoint, version: number, message: Serializable): UdpStream {
    const stream = new UdpStream(this, endpoint, version);
    stream.send(message);
    return stream;
  }

  private handleMessage = (data: Uint8Array, rinfo: AddressInfo) => {
    const type = data[0];

    const buffer = Buffer.from(data, 1, data.byteLength - 1);

    if (type === GENERAL) {
      const serializer = new BufferSerializer(this.version, buffer);
      const obj = this.oracle.serialize(serializer, null);
      const id = this.oracle.identify(obj);
      const handler = this.handlers.get(id);
      handler(obj);
    } else if (type === STREAM) {
      const remoteId = `${rinfo.address}:${rinfo.port}`;
      const endpoint = this.streams.get(remoteId);
      if (!endpoint) {
        const stream = new UdpStream(this, rinfo, 0);
        const serializer = new BufferSerializer(stream.version, buffer);
        stream.receive(serializer, (msg) => {
          const id = this.oracle.identify(msg);
          const connectHandler = this.connectHandlers.get(id);
          const userData = connectHandler(msg, stream);
          if(userData) {
            this.streams.set(remoteId, { stream, userData });
          }
        });
      } else {
        const { stream, userData } = endpoint;
        const serializer = new BufferSerializer(stream.version, buffer);
        stream.receive(serializer, (item: Serializable) => {
          const id = this.oracle.identify(item);
          const streamHandler = this.streamHandlers.get(id);
          streamHandler(item, userData, stream);
        });
      }
    }
  }

  sendStream(stream: UdpStream) {
    const serializer = new BufferSerializer(stream.version, 1000);
    serializer.uint8(STREAM);
    stream.serialize(serializer);
    this.socket.send(serializer.getBuffer(), stream.remote.port, stream.remote.address);
  }

  send(to: EndPoint, msg: Serializable) {
    const serializer = new BufferSerializer(this.version, 1000);
    serializer.uint8(GENERAL);
    this.oracle.serialize(serializer, msg);
    this.socket.send(serializer.getBuffer(), to.port, to.address);
  }
}
