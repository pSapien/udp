import { createSocket, Socket } from 'dgram';

type CacheSocket = {
  socket: Socket,
  reuseHandle: null | typeof timeout,
  ready: boolean,
  onBind: (s: Socket) => void,
}

type Port = number;

const cachedSockets = new Map<Port, CacheSocket>();
const SOCKET_CLOSE_TIMEOUT = 50;

export function openSocket(port: number, onBind: CacheSocket["onBind"]) {
  if (!port) {
    const soc = createSocket('udp4');
    soc.bind();
    return soc.close()
  }

  const cachedSocket = cachedSockets.get(port);

  if (cachedSocket) {
    if (!cachedSocket.reuseHandle) {
      throw new Error('Socket is already in used');
    }

    clearTimeout(cachedSocket.reuseHandle);
    cachedSocket.reuseHandle = null;
    if (cachedSocket.ready) {
      cachedSocket.onBind(cachedSocket.socket);
    }

    return () => closeSocket(cachedSocket.socket);
  }

  const newSocket = createSocket('udp4');
  cachedSockets.set(port, { 
    reuseHandle: null,
    socket: newSocket,
    ready: false,
    onBind,
  });

  newSocket.bind(port, () => {
    const existing = cachedSockets.get(port);
    if (existing) {
      existing.ready = true;
      existing.onBind(existing.socket);
    }
  });

  return () => closeSocket(port);
}

export function closeSocket(port: number, timeout: number = SOCKET_CLOSE_TIMEOUT) {
  const cachedSocket = cachedSockets.get(port);

  cachedSocket.socket.removeAllListeners();
  cachedSocket.reuseHandle = setTimeout(() => {
    cachedSocket.reuseHandle = null;
    cachedSocket.socket.close(() => {
      cachedSockets.delete(cachedSocket.socket.address().port);
    });
  }, timeout);
}

