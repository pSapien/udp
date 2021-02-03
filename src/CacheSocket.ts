import { createSocket, Socket } from 'dgram';

type CacheSocket = {
  socket: Socket,
  reuseHandle: null | typeof Timeout,
}

type Port = number;

const cachedSockets = new Map<Port, CacheSocket>();
const SOCKET_CLOSE_TIMEOUT = 50;

export function openSocket(port: number) {
  if (!port) {
    const soc = createSocket('udp4');
    soc.bind();
    return soc;
  }

  const cachedSocket = cachedSockets.get(port);

  if (cachedSocket) {
    if (!cachedSocket.reuseHandle) {
      throw new Error('Socket is already in used');
    }

    clearTimeout(cachedSocket.reuseHandle);
    cachedSocket.reuseHandle = null;

    return cachedSocket.socket;
  }

  const newSocket = createSocket('udp4');
  cachedSockets.set(port, { 
    reuseHandle: null,
    socket: newSocket,
  });

  newSocket.bind(port);

  return newSocket;
}

export function closeSocket(portOrSocket: number | Socket, timeout: number = SOCKET_CLOSE_TIMEOUT) {
  if (portOrSocket instanceof Socket) {
    portOrSocket.close();
    return;
  }

  const cachedSocket = cachedSockets.get(portOrSocket);
  cachedSocket.reuseHandle = setTimeout(() => {
    cachedSocket.reuseHandle = null;
    cachedSocket.socket.close(() => {
      cachedSockets.delete(cachedSocket.socket.address().port);
    });
  }, timeout);
}