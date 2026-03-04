export type ClientConnection = {
  send: (data: ArrayBuffer) => void;
  close?: (code?: number, reason?: string) => void;
};
