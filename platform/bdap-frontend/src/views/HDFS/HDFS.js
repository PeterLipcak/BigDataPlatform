import React, {Component} from 'react';
import { Redirect } from 'react-router-dom'
import {
  Badge,
  Card,
  CardBody,
  Button,
  CardHeader,
  Col,
  Row,
  Jumbotron,
  ModalHeader,
  ModalBody,
  ModalFooter,
  FormGroup,
  Table,
  Alert
} from 'reactstrap';
import {serverIpPort, formatBytes} from './../../utils/Utils';
import {AppSwitch} from '@coreui/react'
import Dropzone from 'react-dropzone'
import {FilePond, registerPlugin} from 'react-filepond';
import 'filepond/dist/filepond.min.css';
import codeEditor from './../../assets/img/code_editor.png'
import flinkIcon from './../../assets/img/flink_logo.png'

import {hdfsServerIpPort} from './../../utils/Utils';

class Processing extends Component {
  constructor(props) {
    super(props);

    this.state = {
      redirect: false
    };
  };

  setRedirect = () => {
    this.setState({
      redirect: true
    })
  };

  renderRedirect = () => {
    window.open(hdfsServerIpPort, "_blank")
    return <Redirect to='/processing'/>
  };


  render() {
    return (
      <div className="animated fadeIn">
        {this.renderRedirect()}
      </div>
    );
  }
}

export default Processing;
