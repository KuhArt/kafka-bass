const debug = require('debug')('sequentialBus');
const EventEmitter = require('events');

const defaultConfig = {
    maxQueueSize: 100,
}

const STATES = {
  STARTED: 'STARTED',
  STOPPED: 'STOPPED',
}

// move commit functionality to handler

class Bus extends EventEmitter {
  constructor (config = {}) {
      super();
      this.config = {...defaultConfig, ...config };
      this.events = [];
      this.handlers = {};
      this.state = STATES.STOPPED;

      debug(`init bus. config: ${JSON.stringify(config)}`);
  }

  push(event) {
    debug(`push event. event: ${JSON.stringify(event)}`);
    this.events.push(event);

    if (this.config.maxQueueSize <= this.events.length) {
      debug(`max queue size reached.`);
        this.emit('maxQueueSizeReached');
    }  

    if (this.state !== STATES.STARTED) {
      this.resume();
      this.process();
    }
  }

  on(eventName, handler) {
    this.handlers[eventName] = this.handlers[eventName] 
    ? this.handlers[eventName].concat(handler) 
    : [handler];
  }

  async process () {
    if (this.events.length) {
      const event = this.events.shift();
      debug(`process event. event: ${JSON.stringify(event)}`);

       const { message } = event;
       
       const messageData = JSON.parse(Buffer.from(message.value).toString());
       debug(`parse message data. message: ${JSON.stringify(messageData)}`);
  
      const handlers = this
        .handlers[messageData.type] || [];
        
      const handlersPromise = handlers
       .reduce(
        (promise, handler) => promise
        .then(() => handler(event))
        .catch((err) => {
          debug(`failed event processing. event: ${JSON.stringify(event)}`);
          this.emit('failed', err, event)
        }),
        Promise.resolve()
      );

      await handlersPromise;
      debug(`processed event. event: ${JSON.stringify(event)}`);
      this.emit('processed', event);

       
      await this.process();
    } else {
      debug(`bus is empty.`);
      this.pause();
      this.emit('empty');
    }
  }

  pause() {
    this.state = STATES.STOPPED;
  }

  resume() {
   this.state = STATES.STARTED;
  }

  flush () {
    debug(`flush queue.`);
    this.events = [];
    this.pause();
    debug(`bus is empty.`);
    this.emit('empty');
  }
}

module.exports = Bus;