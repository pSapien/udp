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
  private attempts: number = 0;
  private maxAttempts: number = 10;
  private closing: boolean = false;
  private onClose: () => void;

  constructor(socket: UdpSocket<any>, remote: EndPoint, version: number, onClose: () => void) {
    this.socket = socket;
    this.version = version;
    this.remote = remote;
    this.onClose = onClose;
  }

  close() {
    if (this.closing) return;
    this.closing = true;
    this.maxAttempts = 5;

    // Initiate a send for closing
    this.send();
  }

  end() {
    this.closing = true;
    if (this.retryHandle) {
      clearTimeout(this.retryHandle);
      this.retryHandle = null;
    }

    // Perform a cleanup
    if (this.sendHandle) {
      clearImmediate(this.sendHandle);
      this.sendHandle = null;
    }

    // Trigger a close event, making sure it is called once and only once
    if (this.onClose) {
      const k = this.onClose;
      this.onClose = null;
      k();
    }
  }

  receive(serializer: Serializer, handler: (item: Serializable) => void): void {
    const remoteAck = serializer.uint16(0);
    console.log('Remote ack', remoteAck);

    // Remove all items that are acked
    while (this.items.length) {
      const k = this.items[0];
      if (k.seq > remoteAck) break;

      // Remove the items that have been acked
      this.items.shift();
    }

    // Clean up the attempts count, since we received something
    this.attempts = 0;

    if (remoteAck === 0xffff) {
      this.end();
      return;
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
      if (seq === 0xffff) {
        this.remoteSeq = seq;
        this.close();
        this.maxAttempts = 1;
        break;
      }

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
    let remaining = this.items.length;
    for (let i = 0; i < this.items.length; i += 1) {
      const k = this.items[i];
      // Mark the serializer position for reverting back in case of error
      const revert = serializer.mark();
      try {
        serializer.uint16(k.seq);
        this.socket.oracle.serialize(serializer, k.item);
        remaining -= 1;
      } catch (err) {
        revert();
        break;
      }
    }

    // Attach the close seq id at the end
    if (this.closing && !remaining) {
      serializer.uint16(0xffff);
    } else {
      serializer.uint16(0);
    }
  }

  send(item?: Serializable) {
    // Allow additional items only if we are not already closing
    if (item && !this.closing) {
      this.localSeq += 1;
      this.items.push({ seq: this.localSeq, item });
    }

    // If a send is already initiated just ignore
    if (this.sendHandle) return;

    // In case of high attempts, assume a closed connection
    if (this.attempts >= this.maxAttempts) {
      this.end();
      return;
    }

    this.sendHandle = setImmediate(() => {
      this.attempts += 1;
      console.log('Send to stream attempt', this.attempts, this.maxAttempts);
      this.socket.sendStream(this);

      // Clear the handle, once the send is complete
      this.sendHandle = null;

      // Setup a retry after few seconds (not for ack msg)
      if (this.items.length || this.closing) {
        if (this.retryHandle) clearTimeout(this.retryHandle);
        this.retryHandle = setTimeout(() => this.send(), this.retryInterval);
        if (this.retryInterval < MAX_RETRY_INTERVAL) this.retryInterval += 500;
      } else {
        // Clear the attempts count, after sending the ack only packet
        this.attempts = 0;
      }
    });
  }
}

