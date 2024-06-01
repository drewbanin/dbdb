import { useEffect } from 'react';
import { EventEmitter } from 'eventemitter3';

const Emitter = new EventEmitter();
const useSub = (event, callback) => {
  const unsubscribe = () => {
    Emitter.off(event, callback);
  };

  useEffect(() => {
    Emitter.on(event, callback);
    return unsubscribe;
  });

  return unsubscribe;
};

const usePub = () => {
  return (event, data) => {
    Emitter.emit(event, data);
  };
};

export { useSub, usePub };
