import React from 'react';


const Ingestion = React.lazy(() => import('./views/Ingestion/Ingestion'));
const Processing = React.lazy(() => import('./views/Processing/Processing'));
const CodeEditor = React.lazy(() => import('./views/CodeEditor/CodeEditor'));
const HDFS = React.lazy(() => import('./views/HDFS/HDFS'));

const routes = [
  { path: '/', exact: true, name: 'Home' },
  { path: '/processing', exact: true, name: 'Processing', component: Processing },
  { path: '/processing/codeeditor', name: 'CodeEditor', component: CodeEditor },
  { path: '/ingestion', name: 'Ingestion', component: Ingestion },
  { path: '/hdfs', name: 'HDFS', component: HDFS },
];

export default routes;
