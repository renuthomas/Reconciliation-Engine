import { Writable } from 'stream';

class WritableCapture extends Writable {
  constructor(options = {}) {
    super(options);
    this.data = '';
    this.events = [];
  }

  _write(chunk, encoding, callback) {
    this.events.push({ chunk: chunk.toString(), encoding });
    this.data += chunk.toString();
    callback();
  }
}

const createWritableCapture = () => new WritableCapture({ decodeStrings: false });

export { createWritableCapture, WritableCapture };
