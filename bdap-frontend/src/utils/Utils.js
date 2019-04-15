import React, {Component} from 'react';

// export const serverIpPort = 'http://10.5.1.247:8080';
// export const flinkServerIpPort = 'http://10.5.1.247:8081';

export const serverIp = 'http://147.251.255.236';
export const serverIpPort = 'http://147.251.255.236:8080';
export const flinkServerIpPort = 'http://147.251.255.236:8081';
export const hdfsServerIpPort = serverIp + ':50070';

// export const serverIpPort = 'http://127.0.0.1:8080';
// export const flinkServerIpPort = 'http://127.0.0.1:8081';

export const formatBytes = (bytes, decimals = 2) => {
  if (bytes === 0) return '0 Bytes';

  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
};
