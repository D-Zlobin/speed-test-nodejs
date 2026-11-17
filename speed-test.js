const net = require('node:net');
const tls = require('node:tls');
const { performance } = require('node:perf_hooks');

const MAX_HEADER_BYTES = 64 * 1024;

class ChunkedResponseParser {
  constructor() {
    this.buffer = Buffer.alloc(0);
    this.state = 'size';
    this.remaining = 0;
    this.complete = false;
  }

  push(chunk) {
    if (this.complete) {
      if (chunk.length > 0) {
        throw new Error('Received data after the final chunk');
      }
      return;
    }

    this.buffer = this.buffer.length > 0
      ? Buffer.concat([this.buffer, chunk])
      : chunk;

    while (this.buffer.length > 0) {
      if (this.state === 'size' || this.state === 'trailers') {
        const lineEnd = this.buffer.indexOf('\r\n');
        if (lineEnd === -1) {
          if (this.buffer.length > MAX_HEADER_BYTES) {
            throw new Error('Chunk metadata is too large');
          }
          return;
        }

        const line = this.buffer.subarray(0, lineEnd).toString('latin1');
        this.buffer = this.buffer.subarray(lineEnd + 2);

        if (this.state === 'trailers') {
          if (line.length === 0) {
            this.complete = true;
            if (this.buffer.length > 0) {
              throw new Error('Received data after the final chunk');
            }
            return;
          }
          continue;
        }

        const sizeText = line.split(';', 1)[0];
        if (!/^[0-9a-fA-F]+$/.test(sizeText)) {
          throw new Error('Invalid chunk size');
        }

        const size = Number.parseInt(sizeText, 16);
        if (!Number.isSafeInteger(size)) {
          throw new Error('Chunk size is too large');
        }

        if (size === 0) {
          this.state = 'trailers';
        } else {
          this.remaining = size;
          this.state = 'data';
        }
        continue;
      }

      if (this.state === 'data') {
        const consumed = Math.min(this.remaining, this.buffer.length);
        this.remaining -= consumed;
        this.buffer = this.buffer.subarray(consumed);
        if (this.remaining === 0) {
          this.state = 'data-crlf';
        }
        continue;
      }

      if (this.buffer.length < 2) {
        return;
      }
      if (this.buffer[0] !== 13 || this.buffer[1] !== 10) {
        throw new Error('Invalid chunk terminator');
      }
      this.buffer = this.buffer.subarray(2);
      this.state = 'size';
    }
  }
}

function parseResponseHeaders(buffer) {
  const lines = buffer.toString('latin1').split('\r\n');
  const statusMatch = /^HTTP\/1\.[01] (\d{3})(?:[ \t].*)?$/.exec(lines[0]);
  if (!statusMatch) {
    throw new Error('Invalid HTTP status line');
  }

  const headers = Object.create(null);
  for (const line of lines.slice(1)) {
    const separator = line.indexOf(':');
    if (separator <= 0) {
      throw new Error('Invalid HTTP header');
    }
    const name = line.slice(0, separator).trim().toLowerCase();
    const value = line.slice(separator + 1).trim();
    headers[name] = headers[name] === undefined
      ? value
      : `${headers[name]},${value}`;
  }

  let expectedBodyBytes = null;
  if (headers['content-length'] !== undefined) {
    if (!/^\d+$/.test(headers['content-length'])) {
      throw new Error('Invalid Content-Length header');
    }
    expectedBodyBytes = Number(headers['content-length']);
    if (!Number.isSafeInteger(expectedBodyBytes)) {
      throw new Error('Content-Length is too large');
    }
  }

  const transferEncoding = headers['transfer-encoding'] || '';
  const chunked = transferEncoding
    .split(',')
    .some((encoding) => encoding.trim().toLowerCase() === 'chunked');

  return {
    statusCode: Number(statusMatch[1]),
    expectedBodyBytes,
    chunked,
  };
}

function parseTarget(value) {
  if (!value) {
    throw new Error('Missing URL. Usage: node speed-test.js <url>');
  }

  let url;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`Invalid URL: ${value}`);
  }

  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`Unsupported protocol: ${url.protocol}`);
  }
  return url;
}

function requestOnce(url) {
  return new Promise((resolve, reject) => {
    const startedAt = performance.now();
    const isTls = url.protocol === 'https:';
    const hostname = url.hostname.startsWith('[')
      ? url.hostname.slice(1, -1)
      : url.hostname;
    const port = url.port ? Number(url.port) : (isTls ? 443 : 80);
    const options = { host: hostname, port };
    const socket = isTls
      ? tls.connect({
        ...options,
        servername: net.isIP(hostname) ? undefined : hostname,
      })
      : net.connect(options);

    const request = [
      `GET ${url.pathname}${url.search} HTTP/1.1`,
      `Host: ${url.host}`,
      'User-Agent: speed-test-nodejs/1.0',
      'Accept: */*',
      'Accept-Encoding: gzip, deflate, br',
      'Connection: close',
      '',
      '',
    ].join('\r\n');

    let settled = false;
    let totalBytes = 0;
    let bodyBytes = 0;
    let headerBuffer = Buffer.alloc(0);
    let response = null;
    let chunkParser = null;

    function fail(error) {
      if (settled) return;
      settled = true;
      socket.destroy();
      reject(error instanceof Error ? error : new Error(String(error)));
    }

    function sendRequest() {
      socket.write(request, 'latin1', (error) => {
        if (error) fail(error);
      });
    }

    socket.once('error', fail);
    socket.once('close', () => {
      if (!settled) {
        fail(new Error('Connection closed before the response completed'));
      }
    });

    if (isTls) {
      socket.once('secureConnect', sendRequest);
    } else {
      socket.once('connect', sendRequest);
    }

    socket.on('data', (chunk) => {
      try {
        totalBytes += chunk.length;

        if (response) {
          bodyBytes += chunk.length;
          if (chunkParser) chunkParser.push(chunk);
          return;
        }

        headerBuffer = headerBuffer.length > 0
          ? Buffer.concat([headerBuffer, chunk])
          : chunk;
        const headerEnd = headerBuffer.indexOf('\r\n\r\n');

        if (headerEnd === -1) {
          if (headerBuffer.length > MAX_HEADER_BYTES) {
            throw new Error('HTTP response headers are too large');
          }
          return;
        }
        if (headerEnd > MAX_HEADER_BYTES) {
          throw new Error('HTTP response headers are too large');
        }

        response = parseResponseHeaders(headerBuffer.subarray(0, headerEnd));
        const body = headerBuffer.subarray(headerEnd + 4);
        headerBuffer = Buffer.alloc(0);
        bodyBytes = body.length;

        if (response.chunked) {
          chunkParser = new ChunkedResponseParser();
          chunkParser.push(body);
        }
      } catch (error) {
        fail(error);
      }
    });

    socket.once('end', () => {
      try {
        if (!response) {
          throw new Error('Incomplete HTTP response headers');
        }
        if (!response.chunked
          && response.expectedBodyBytes !== null
          && bodyBytes !== response.expectedBodyBytes) {
          throw new Error(
            `Incomplete response body: expected ${response.expectedBodyBytes} bytes, received ${bodyBytes}`,
          );
        }
        if (response.chunked && !chunkParser.complete) {
          throw new Error('Incomplete chunked response body');
        }
        if (response.statusCode < 200 || response.statusCode >= 300) {
          throw new Error(`HTTP status ${response.statusCode}`);
        }

        settled = true;
        socket.destroy();
        resolve({
          durationMs: performance.now() - startedAt,
          bytes: totalBytes,
        });
      } catch (error) {
        fail(error);
      }
    });
  });
}

const REQUEST_COUNT = 10;
const PROGRESS_WIDTH = 20;

function updateProgress(completed, activeAttempt = null) {
  if (!process.stderr.isTTY) return;

  const filled = Math.floor((completed / REQUEST_COUNT) * PROGRESS_WIDTH);
  const bar = `${'#'.repeat(filled)}${'-'.repeat(PROGRESS_WIDTH - filled)}`;
  const active = activeAttempt === null
    ? ''
    : ` | Request ${activeAttempt}/${REQUEST_COUNT}`;
  process.stderr.write(`\r\x1b[2K[${bar}] ${completed}/${REQUEST_COUNT}${active}`);
}

function clearProgress() {
  if (process.stderr.isTTY) {
    process.stderr.write('\r\x1b[2K');
  }
}

function finishProgress() {
  if (process.stderr.isTTY) {
    process.stderr.write('\n');
  }
}

async function main() {
  const url = parseTarget(process.argv[2]);
  const results = [];
  let failures = 0;

  for (let attempt = 1; attempt <= REQUEST_COUNT; attempt += 1) {
    updateProgress(attempt - 1, attempt);
    try {
      results.push(await requestOnce(url));
    } catch (error) {
      failures += 1;
      clearProgress();
      console.error(`Request ${attempt}/${REQUEST_COUNT} failed: ${error.message}`);
    }
    updateProgress(attempt);
  }
  finishProgress();

  console.log(`Successful requests: ${results.length}/${REQUEST_COUNT}`);
  console.log(`Failed requests: ${failures}`);

  if (results.length === 0) {
    console.error('No successful responses; cannot calculate request time or speed.');
    process.exitCode = 1;
    return;
  }

  const totalDurationMs = results.reduce((sum, result) => sum + result.durationMs, 0);
  const totalBytes = results.reduce((sum, result) => sum + result.bytes, 0);
  const averageDurationMs = totalDurationMs / results.length;
  const speedMbps = (totalBytes * 8) / ((totalDurationMs / 1000) * 1_000_000);

  console.log(`Average request time: ${averageDurationMs.toFixed(2)} ms`);
  console.log(`Total downloaded: ${totalBytes} bytes (${(totalBytes / 1_000_000).toFixed(2)} MB)`);
  console.log(`Download speed: ${speedMbps.toFixed(2)} Mbps`);

  if (failures > 0) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
