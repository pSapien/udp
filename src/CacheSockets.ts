import dgram from 'dgram';
import { resolve } from 'path';

type Port = number;
type CacheSockInstance = {
  socket: dgram.Socket,
  closeHandle: null | typeof timeout,
  isReady: boolean,
}

const cachedSockets = new Map<Port, CacheSockInstance>();
const CLOSE_SOCKET_TIMEOUT = 20;

export function openSocket(port: Port) {
  if (cachedSockets.has(port)) return cachedSockets.get(port).socket;

  const newSocket = dgram.createSocket('udp4');

  cachedSockets.set(port, {
    socket: newSocket,
    closeHandle: null,
    isReady: false,
  });

  return newSocket;
}

export async function bindSocket(socket: dgram.Socket, port: Port) {
  const cacheSockInstance = cachedSockets.get(port);

  if (cacheSockInstance) {
    if (cacheSockInstance.closeHandle) {
      clearTimeout(cacheSockInstance.closeHandle);
      cacheSockInstance.closeHandle = null;
    }

    if (cacheSockInstance.isReady) return cacheSockInstance.socket;
  }

  return new Promise(() => {
    socket.bind(port, () => {
      cachedSockets.get(port).isReady = true;
    });
  });
}

export function closeSocket(socket: dgram.Socket, timeout = CLOSE_SOCKET_TIMEOUT) {
  const { port } = socket.address();
  const sockInstance = cachedSockets.get(port);

  sockInstance.closeHandle = setTimeout(() => {
    sockInstance.closeHandle = null;
    sockInstance.socket.close(() => {
      cachedSockets.delete(port);
    });
  }, timeout);
}