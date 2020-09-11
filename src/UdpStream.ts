import { Serializable, Serializer, BufferSerializer } from '@bhoos/serialization';
import { UdpSocket } from './UdpSocket';

const MAX_RETRY_INTERVAL = 3000;
const MIN_RETRY_INTERVAL = 500;

export type EndPoint = { port: number, address: string };

export interface Stream {
  readonly version: number;
  close(): void;
  send(item: Serializable): void;
  receive(serializer: Serializer, handler: (item: Serializable) => void): void;
};

export class UdpStream implements Stream {
  private readonly items: Array<{ seq: number, item: Serializable }> = [];

  // Keep track of messages that are being sent to remote
  private localSeq = 0;

  // Keep track of remote messages that have been received so far
  private remoteSeq = 0;

  private readonly socket: UdpSocket<any>;
  readonly remote: EndPoint;
  readonly version: number;

  private sendHandle: ReturnType<typeof setImmediate> = null;
  private retryHandle: ReturnType<typeof setTimeout> = null;
  private retryInterval = MIN_RETRY_INTERVAL; // Perform a retry in 500 ms, failing which perform a retry in 1000 and 1500 and so on

  constructor(socket: UdpSocket<any>, remote: EndPoint, version: number) {
    this.socket = socket;
    this.version = version;
    this.remote = remote;
  }

  close() {
    if (this.retryHandle) {
      clearTimeout(this.retryHandle);
      this.retryHandle = null;
    }

    // Perform a cleanup
    if (this.sendHandle) {
      clearImmediate(this.sendHandle);
      this.sendHandle = null;
    }
  }

  receive(serializer: Serializer, handler: (item: Serializable) => void): void {
    const remoteAck = serializer.uint16(0);
    // Remove all items that are acked
    while (this.items.length) {
      const k = this.items[0];
      if (k.seq > remoteAck) break;

      // Remove the items that have been acked
      this.items.shift();
    }

    // Cleanup the retry interval as soon as something is received, since the ack
    // would be initiated anyhow
    this.retryInterval = MIN_RETRY_INTERVAL;
    if (this.retryHandle) {
      clearTimeout(this.retryHandle);
      this.retryHandle = null;
    }

    do {
      const seq = serializer.uint16(0);
      if (seq === 0) break;

      if (seq > this.remoteSeq) {
        // Something we haven't seen before, initiate an ack seq
        if (!this.sendHandle) this.send();

        this.remoteSeq = seq;
        const item = this.socket.oracle.serialize(serializer, null);
        handler(item);
      }
    } while (true);
  }

  serialize(serializer: Serializer) {
    serializer.uint16(this.remoteSeq);
    for (let i = 0; i < this.items.length; i += 1) {
      const k = this.items[i];
      // Mark the serializer position for reverting back in case of error
      serializer.mark();
      try {
        serializer.uint16(k.seq);
        this.socket.oracle.serialize(serializer, k.item);
      } catch (err) {
        serializer.revert();
        serializer.uint16(0);
      }
    }
  }

  send(item?: Serializable) {
    if (item) {
      this.localSeq += 1;
      this.items.push({ seq: this.localSeq, item });
    }

    // If a send is already initiated just ignore
    if (this.sendHandle) return;

    this.sendHandle = setImmediate(() => {
      this.socket.sendStream(this);

      // Clear the handle, once the send is complete
      this.sendHandle = null;

      // Setup a retry after few seconds (not for ack msg)
      if (this.items.length) {
        if (this.retryHandle) clearTimeout(this.retryHandle);
        this.retryHandle = setTimeout(() => this.send(), this.retryInterval);
        if (this.retryInterval < MAX_RETRY_INTERVAL) this.retryInterval += 500;
      }
    });
  }
}

