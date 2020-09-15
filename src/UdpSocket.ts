import { createSocket, Socket } from 'dgram';
import { AddressInfo } from 'net';
import { EndPoint, UdpStream } from './UdpStream';
import { BufferSerializer, Serializable, Oracle } from '@bhoos/serialization';

const GENERAL = 0;
const STREAM = 1;

function getEndPointId(endpoint: EndPoint) {
  return `${endpoint.address}:${endpoint.port}`;
}

export class UdpSocket<S = UdpStream> {
  private socket: Socket;
  private version: number;
  readonly oracle: Oracle;

  private handlers = new Map<number, (msg: Serializable) => void>();
  private streamHandlers = new Map<number, (msg: Serializable, userData: S, stream: UdpStream) => void>();
  private connectHandlers = new Map<number, (msg: Serializable, stream: UdpStream) => Promise<S>>();
  private closeHandler: (userData: S, stream: UdpStream) => void;
  private stream: UdpStream;
  private streams: Map<string, { stream: UdpStream, userData: S}>;
  private closing: boolean = false;

  constructor(oracle: Oracle, version: number) {
    this.version = version;
    this.oracle = oracle;
    this.socket = createSocket('udp4');
    this.socket.on('message', this.handleMessage);
  }

  listen(port: number) {
    this.socket.bind(port);
    this.streams = new Map<string, { stream: UdpStream, userData: S}>();
  }

  connect(endpoint: EndPoint, version: number, message: Serializable): UdpStream {
    const stream = new UdpStream(this, endpoint, version, () => {
      this.stream = null;
      this.closing = true;
      this.handleClose(null, stream);
    });
    this.stream = stream;
    this.stream.send(message);
    return this.stream;
  }

  private handleClose(userData: S, stream: UdpStream) {
    if (this.closeHandler) this.closeHandler(userData, stream);
    if (this.closing) {
      if (!this.stream && (!this.streams || this.streams.size === 0)) this.end();
    }
  }

  close() {
    if (this.closing) return;
    this.closing = true;

    // close will end if there aren't any streams associated with the socket
    if (!this.stream && (!this.streams || this.streams.size === 0)) this.end();

    // Attempt to close any direct streams
    if (this.stream) this.stream.close();

    // Initiate close on all the connected streams
    if (this.streams) this.streams.forEach(({ stream }) => stream.close());
  }

  end() {
    this.socket.off('message', this.handleMessage);
    this.socket.close();
  }

  on<T extends Serializable>(clazz: new (...args:any[]) => T, handler: (msg: T) => void) {
    const id = this.oracle.id(clazz);
    this.handlers.set(id, handler);
  }

  onClose(handler: (userData: S, stream: UdpStream) => void) {
    if (this.closeHandler) {
      throw new Error('Close handler is already defined');
    }
    this.closeHandler = handler;
  }

  onConnect<T extends Serializable>(clazz: new (...args: any[]) => T, handler: (msg: T, stream: UdpStream) => Promise<S>) {
    const id = this.oracle.id(clazz);
    this.connectHandlers.set(id, handler);
  }

  onStream<T extends Serializable>(clazz: new (...args:any[]) => T, handler: (msg: T, user: S) => void) {
    const id = this.oracle.id(clazz);
    this.streamHandlers.set(id, handler);
  }

  raiseError(err: Error) {
    console.log(err);
  }

  private handleMessage = (data: Uint8Array, rinfo: AddressInfo) => {
    const type = data[0];
    console.log(`Rx: ${rinfo.address}:${rinfo.port}, ${type}, ${data.length}`, data);

    const buffer = Buffer.from(data);
    if (type === GENERAL) {
      const serializer = new BufferSerializer(this.version, buffer, 1);
      const obj = this.oracle.serialize(serializer, null);
      const id = this.oracle.identify(obj);
      const handler = this.handlers.get(id);
      handler(obj);
    } else if (type === STREAM) {
      if (this.stream) {
        const serializer = new BufferSerializer(this.stream.version, buffer, 1);
        this.stream.receive(serializer, (item: Serializable) => {
          const id = this.oracle.identify(item);
          const streamHandler = this.streamHandlers.get(id);
          if (!streamHandler) {
            this.raiseError(new Error(`No stream handler found for ${id}`));
          } else {
            streamHandler(item, null, this.stream);
          }
        });
      } else {
        const remoteId = getEndPointId(rinfo);
        const endpoint = this.streams.get(remoteId);
        if (!endpoint) {
          const stream = new UdpStream(this, rinfo, 0, () => {
            // Inform the socket, in case the stream has been registered
            const ep = this.streams.get(remoteId);
            // It is possible for the endpoint be a different stream, so make sure
            if (ep.stream === stream) {
              this.streams.delete(remoteId);
              this.handleClose(ep.userData, ep.stream);
            }
          });
          const serializer = new BufferSerializer(stream.version, buffer, 1);
          stream.receive(serializer, async (msg) => {
            const id = this.oracle.identify(msg);
            const connectHandler = this.connectHandlers.get(id);
            if (!connectHandler) {
              stream.close();
              this.raiseError(new Error(`No connection handler found ${remoteId}, ${id}`));
            } else {
              const userData = await connectHandler(msg, stream);
              // Since the connect handler is async, it's possible that another request might
              // have been received within that time
              if(userData && !this.streams.has(remoteId)) {
                this.streams.set(remoteId, { stream, userData });
              } else {
                // Get rid of the stream (without closing), just need to release the resources
                stream.end();
              }
            }
          });
        } else {
          const { stream, userData } = endpoint;
          const serializer = new BufferSerializer(stream.version, buffer, 1);
          stream.receive(serializer, (item: Serializable) => {
            const id = this.oracle.identify(item);
            const streamHandler = this.streamHandlers.get(id);
            if (!streamHandler) {
              this.raiseError(new Error(`No stream handler found for ${id}`));
            } else {
              streamHandler(item, userData, stream);
            }
          });
        }
      }
    }
  }

  sendStream(stream: UdpStream) {
    const serializer = new BufferSerializer(stream.version, 1000);
    serializer.uint8(STREAM);
    stream.serialize(serializer);
    const buffer = serializer.getBuffer();
    this.socket.send(buffer, 0, serializer.length, stream.remote.port, stream.remote.address);
    console.log(`Sending data to ${stream.remote.address}:${stream.remote.port}, ${serializer.length} bytes`);
  }

  send(to: EndPoint, msg: Serializable) {
    const serializer = new BufferSerializer(this.version, 1000);
    serializer.uint8(GENERAL);
    this.oracle.serialize(serializer, msg);
    this.socket.send(serializer.getBuffer(), 0, serializer.length, to.port, to.address);
  }
}
