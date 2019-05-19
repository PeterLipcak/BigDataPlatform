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

import {flinkServerIpPort} from './../../utils/Utils';

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
    if (this.state.redirect) {
      return <Redirect to='/processing/codeeditor'/>
    }
  };


  render() {
    return (
      <div className="animated fadeIn">
        {this.renderRedirect()}

        <Row>
          <Col>
            <Jumbotron onClick={this.setRedirect} className="processingJumbotronBody cardBody pointer">
              <div className="img-with-text">
                <img className="processingLogo" src={codeEditor}/>
                <strong>Code Editor</strong>
              </div>
              <hr/>
              <h4>This is a simple code editor that allows you to submit processing script to Apache Flink</h4>
            </Jumbotron>
          </Col>

          <Col>
            <Jumbotron onClick={()=> window.open(flinkServerIpPort, "_blank")} className="processingJumbotronBody cardBody pointer">
              <div className="img-with-text">
                <img className="processingLogo" src={flinkIcon}/>
                <strong>Apache Flink</strong>
              </div>
              <hr/>
              <h4>Flink UI allows you to upload and run processing scripts that are packaged with all dependencies in
                jar file.</h4>
            </Jumbotron>
          </Col>
        </Row>
      </div>
    );
  }
}

export default Processing;
