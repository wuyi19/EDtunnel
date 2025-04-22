// EDtunnel - Optimized Cloudflare Worker VLESS Proxy
// @ts-ignore
import { connect } from 'cloudflare:sockets';

// ======================================
// Configuration Manager
// ======================================
class ConfigManager {
  constructor(env) {
    this.userIDs = this.parseUserIDs(env.UUID || 'd342d11e-d424-4583-b36e-524ab1f0afa4');
    this.proxyIPs = this.parseProxyIPs(env.PROXYIP || 'cdn.xn--b6gac.eu.org:443,cdn-all.xn--b6gac.eu.org:443');
    this.socks5Config = this.parseSocks5(env.SOCKS5 || '');
    this.socks5Relay = env.SOCKS5_RELAY === 'true' || false;
    this.validateConfig();
  }

  parseUserIDs(uuid) {
    const ids = uuid.split(',').map(id => id.trim());
    if (!ids.every(isValidUUID)) {
      throw new Error('Invalid UUID format detected');
    }
    return ids;
  }

  parseProxyIPs(proxyIP) {
    return proxyIP.split(',').map(addr => {
      const [host, port = '443'] = addr.trim().split(':');
      return { host, port };
    });
  }

  parseSocks5(socks5) {
    if (!socks5) return null;
    
    const [auth, hostport] = socks5.split('@').reverse();
    const [host, port = '1080'] = hostport.split(':');
    
    if (!host || isNaN(port)) {
      throw new Error('Invalid SOCKS5 address format');
    }

    if (auth) {
      const [username, password] = auth.split(':');
      return { username, password, host, port: parseInt(port) };
    }
    
    return { host, port: parseInt(port) };
  }

  validateConfig() {
    if (this.userIDs.length === 0) {
      throw new Error('At least one valid UUID is required');
    }
  }

  getRandomProxy() {
    return this.proxyIPs[Math.floor(Math.random() * this.proxyIPs.length)];
  }
}

// ======================================
// Connection Manager
// ======================================
class ConnectionManager {
  constructor() {
    this.connections = new Map();
    this.connectionTimeout = 30000; // 30 seconds
  }

  async getConnection(address, port) {
    const key = `${address}:${port}`;
    this.cleanupStaleConnections();

    if (this.connections.has(key)) {
      const conn = this.connections.get(key);
      if (conn.readyState === 'open') return conn;
    }

    const newConn = await connect({ hostname: address, port });
    newConn.lastUsed = Date.now();
    this.connections.set(key, newConn);

    // Set up cleanup on close
    newConn.addEventListener('close', () => this.connections.delete(key));
    
    return newConn;
  }

  cleanupStaleConnections() {
    const now = Date.now();
    for (const [key, conn] of this.connections.entries()) {
      if (now - conn.lastUsed > this.connectionTimeout) {
        conn.close();
        this.connections.delete(key);
      }
    }
  }
}

// ======================================
// Protocol Handler
// ======================================
class ProtocolHandler {
  static processVLessHeader(buffer, userIDs) {
    if (buffer.byteLength < 24) {
      return { hasError: true, message: 'Invalid header length' };
    }

    const dataView = new DataView(buffer);
    const slicedBuffer = new Uint8Array(buffer.slice(1, 17));
    const uuid = unsafeStringify(slicedBuffer);

    if (!userIDs.includes(uuid)) {
      return { hasError: true, message: 'Unauthorized user' };
    }

    const optLength = dataView.getUint8(17);
    const command = dataView.getUint8(18 + optLength);

    if (command !== 1 && command !== 2) {
      return { hasError: true, message: 'Unsupported command' };
    }

    const portIndex = 18 + optLength + 1;
    const portRemote = dataView.getUint16(portIndex);
    const addressType = dataView.getUint8(portIndex + 2);

    let addressRemote, rawDataIndex;
    switch (addressType) {
      case 1: // IPv4
        addressRemote = new Uint8Array(buffer.slice(portIndex + 3, portIndex + 7)).join('.');
        rawDataIndex = portIndex + 7;
        break;
      case 2: { // Domain
        const domainLength = dataView.getUint8(portIndex + 3);
        addressRemote = new TextDecoder().decode(
          buffer.slice(portIndex + 4, portIndex + 4 + domainLength)
        );
        rawDataIndex = portIndex + 4 + domainLength;
        break;
      }
      case 3: // IPv6
        addressRemote = Array.from({ length: 8 }, (_, i) => 
          dataView.getUint16(portIndex + 3 + i * 2).toString(16)
        ).join(':');
        rawDataIndex = portIndex + 19;
        break;
      default:
        return { hasError: true, message: 'Invalid address type' };
    }

    return {
      hasError: false,
      addressRemote,
      addressType,
      portRemote,
      rawDataIndex,
      isUDP: command === 2,
      protocolVersion: new Uint8Array([dataView.getUint8(0)])
    };
  }
}

// ======================================
// WebSocket Stream Handler
// ======================================
class WebSocketStreamHandler {
  static createReadableStream(webSocket, earlyDataHeader) {
    let buffer = [];
    let controller;
    let cancelReason;

    const stream = new ReadableStream({
      start(c) {
        controller = c;
        // Flush buffered data
        if (buffer.length > 0) {
          buffer.forEach(chunk => controller.enqueue(chunk));
          buffer = [];
        }
      },
      pull() {
        // Implement backpressure if needed
      },
      cancel(reason) {
        cancelReason = reason;
        safeCloseWebSocket(webSocket);
      }
    });

    webSocket.addEventListener('message', (event) => {
      if (controller) {
        controller.enqueue(event.data);
      } else {
        buffer.push(event.data);
      }
    });

    webSocket.addEventListener('close', () => {
      if (controller) controller.close();
      if (cancelReason) console.log('Stream canceled:', cancelReason);
    });

    webSocket.addEventListener('error', (err) => {
      if (controller) controller.error(err);
    });

    // Handle early data
    if (earlyDataHeader) {
      const { earlyData } = base64ToArrayBuffer(earlyDataHeader);
      if (earlyData) {
        if (controller) {
          controller.enqueue(earlyData);
        } else {
          buffer.push(earlyData);
        }
      }
    }

    return stream;
  }
}

// ======================================
// Main Worker Implementation
// ======================================
export default {
  async fetch(request, env, ctx) {
    try {
      // Initialize configuration
      const config = new ConfigManager(env);
      const connectionManager = new ConnectionManager();
      const rateLimiter = new RateLimiter();
      
      // Apply rate limiting
      const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
      if (!rateLimiter.check(clientIP)) {
        return new Response('Too many requests', { status: 429 });
      }

      // Handle WebSocket upgrade
      if (request.headers.get('Upgrade') === 'websocket') {
        return this.handleWebSocketUpgrade(request, config, connectionManager);
      }

      // Handle HTTP requests
      return this.handleHttpRequest(request, config);
    } catch (error) {
      console.error('Worker error:', error);
      return new Response(error.message, { status: 500 });
    }
  },

  async handleWebSocketUpgrade(request, config, connectionManager) {
    // @ts-ignore
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept();
    const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';

    const readableStream = WebSocketStreamHandler.createReadableStream(
      server, 
      earlyDataHeader
    );

    let remoteSocketWrapper = { value: null };
    let isDns = false;

    await readableStream.pipeTo(new WritableStream({
      async write(chunk) {
        if (isDns) {
          return this.handleDnsQuery(chunk, server);
        }

        if (remoteSocketWrapper.value) {
          const writer = remoteSocketWrapper.value.writable.getWriter();
          await writer.write(chunk);
          writer.releaseLock();
          return;
        }

        const headerInfo = ProtocolHandler.processVLessHeader(
          chunk, 
          config.userIDs
        );

        if (headerInfo.hasError) {
          throw new Error(headerInfo.message);
        }

        if (headerInfo.isUDP) {
          if (headerInfo.portRemote === 53) {
            isDns = true;
            return this.handleDnsQuery(
              chunk.slice(headerInfo.rawDataIndex), 
              server,
              headerInfo.protocolVersion
            );
          }
          throw new Error('UDP only supported for DNS (port 53)');
        }

        await this.handleTcpConnection(
          remoteSocketWrapper,
          headerInfo,
          chunk,
          server,
          config,
          connectionManager
        );
      },
      
      async handleTcpConnection(
        remoteSocketWrapper,
        headerInfo,
        chunk,
        webSocket,
        config,
        connectionManager
      ) {
        const { addressRemote, portRemote, rawDataIndex } = headerInfo;
        const rawClientData = chunk.slice(rawDataIndex);
        const protocolResponseHeader = new Uint8Array([headerInfo.protocolVersion[0], 0]);

        try {
          const tcpSocket = await this.connectToRemote(
            addressRemote, 
            portRemote,
            config,
            connectionManager
          );

          remoteSocketWrapper.value = tcpSocket;
          
          // Write initial data
          const writer = tcpSocket.writable.getWriter();
          await writer.write(rawClientData);
          writer.releaseLock();

          // Pipe remote data to WebSocket
          await tcpSocket.readable.pipeTo(new WritableStream({
            write(data) {
              if (webSocket.readyState === WS_READY_STATE_OPEN) {
                webSocket.send(
                  protocolResponseHeader 
                    ? new Blob([protocolResponseHeader, data]).arrayBuffer()
                    : data
                );
                protocolResponseHeader = null;
              }
            },
            close() {
              safeCloseWebSocket(webSocket);
            }
          }));
        } catch (error) {
          console.error('TCP connection failed:', error);
          safeCloseWebSocket(webSocket);
        }
      },

      async connectToRemote(address, port, config, connectionManager) {
        if (config.socks5Relay && config.socks5Config) {
          return this.connectViaSocks5(address, port, config.socks5Config);
        }
        
        const proxy = config.getRandomProxy();
        return connectionManager.getConnection(proxy.host, proxy.port);
      },

      async connectViaSocks5(address, port, socksConfig) {
        // Implement SOCKS5 connection logic
        // ...
      },

      async handleDnsQuery(data, webSocket, protocolHeader) {
        try {
          const dnsServer = '1.1.1.1';
          const dnsPort = 53;
          
          const tcpSocket = await connect({ hostname: dnsServer, port: dnsPort });
          const writer = tcpSocket.writable.getWriter();
          await writer.write(data);
          writer.releaseLock();

          await tcpSocket.readable.pipeTo(new WritableStream({
            write(chunk) {
              if (webSocket.readyState === WS_READY_STATE_OPEN) {
                webSocket.send(
                  protocolHeader
                    ? new Blob([protocolHeader, chunk]).arrayBuffer()
                    : chunk
                );
              }
            }
          }));
        } catch (error) {
          console.error('DNS query failed:', error);
        }
      }
    }));

    return new Response(null, {
      status: 101,
      // @ts-ignore
      webSocket: client,
    });
  },

  handleHttpRequest(request, config) {
    const url = new URL(request.url);
    const host = request.headers.get('Host') || '';
    
    // Handle /cf endpoint
    if (url.pathname === '/cf') {
      return new Response(JSON.stringify(request.cf, null, 2), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // Handle user-specific paths
    const userPathMatch = url.pathname.match(/\/(?:sub|bestip)?\/?([0-9a-f-]{36})/i);
    if (userPathMatch && config.userIDs.includes(userPathMatch[1])) {
      const userID = userPathMatch[1];
      
      if (url.pathname.startsWith('/sub/')) {
        return this.handleSubscriptionRequest(userID, host, config);
      } 
      
      if (url.pathname.startsWith('/bestip/')) {
        return fetch(`https://bestip.06151953.xyz/auto?host=${host}&uuid=${userID}&path=/`);
      }
      
      return this.handleConfigPage(userID, host, config);
    }

    // Default response (disguised cloud drive)
    return this.generateDrivePage(host);
  },

  handleSubscriptionRequest(userID, host, config) {
    const format = new URL(request.url).searchParams.get('format');
    const subscriptionContent = this.generateSubscription(userID, host, config);
    
    if (format === 'clash') {
      return new Response(this.generateClashConfig(userID, host, config), {
        headers: { 'Content-Type': 'text/yaml; charset=utf-8' }
      });
    }
    
    return new Response(subscriptionContent, {
      headers: { 'Content-Type': 'text/plain; charset=utf-8' }
    });
  },

  generateConfigPage(userID, host, config) {
    // Implement config page generation
    // ...
  },

  generateSubscription(userID, host, config) {
    // Implement subscription generation
    // ...
  },

  generateClashConfig(userID, host, config) {
    // Implement Clash config generation
    // ...
  },

  generateDrivePage(host) {
    // Implement cloud drive page
    // ...
  }
};

// ======================================
// Utility Functions
// ======================================
function isValidUUID(uuid) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(uuid);
}

function base64ToArrayBuffer(base64Str) {
  if (!base64Str) return { earlyData: null, error: null };
  try {
    base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
    const binaryStr = atob(base64Str);
    const buffer = new ArrayBuffer(binaryStr.length);
    const view = new Uint8Array(buffer);
    for (let i = 0; i < binaryStr.length; i++) {
      view[i] = binaryStr.charCodeAt(i);
    }
    return { earlyData: buffer, error: null };
  } catch (error) {
    return { earlyData: null, error };
  }
}

function safeCloseWebSocket(socket) {
  try {
    if (socket.readyState === WS_READY_STATE_OPEN || 
        socket.readyState === WS_READY_STATE_CLOSING) {
      socket.close();
    }
  } catch (error) {
    console.error('Error closing WebSocket:', error);
  }
}

const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;

// Rate Limiter Class
class RateLimiter {
  constructor() {
    this.requests = new Map();
    this.windowSize = 60000; // 1 minute
    this.maxRequests = 100; // Max requests per window
  }

  check(ip) {
    const now = Date.now();
    let record = this.requests.get(ip);

    if (!record) {
      record = { count: 1, startTime: now };
      this.requests.set(ip, record);
      return true;
    }

    if (now - record.startTime > this.windowSize) {
      record.count = 1;
      record.startTime = now;
      return true;
    }

    if (record.count++ >= this.maxRequests) {
      return false;
    }

    return true;
  }
}

// UUID stringify helper
const byteToHex = Array.from({ length: 256 }, (_, i) => (i + 0x100).toString(16).slice(1));

function unsafeStringify(arr, offset = 0) {
  return [
    byteToHex[arr[offset]],
    byteToHex[arr[offset + 1]],
    byteToHex[arr[offset + 2]],
    byteToHex[arr[offset + 3]], '-',
    byteToHex[arr[offset + 4]],
    byteToHex[arr[offset + 5]], '-',
    byteToHex[arr[offset + 6]],
    byteToHex[arr[offset + 7]], '-',
    byteToHex[arr[offset + 8]],
    byteToHex[arr[offset + 9]], '-',
    byteToHex[arr[offset + 10]],
    byteToHex[arr[offset + 11]],
    byteToHex[arr[offset + 12]],
    byteToHex[arr[offset + 13]],
    byteToHex[arr[offset + 14]],
    byteToHex[arr[offset + 15]]
  ].join('').toLowerCase();
}
