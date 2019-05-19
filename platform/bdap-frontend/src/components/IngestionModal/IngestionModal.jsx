import React, {Component} from 'react';
import {
  Badge,
  Card,
  CardBody,
  Button,
  Form,
  Col,
  Row,
  Modal,
  ModalHeader,
  ModalBody,
  ModalFooter,
  FormGroup,
  Label,
  Input,
  Alert
} from 'reactstrap';
import {formatBytes} from './../../utils/Utils';
import {serverIpPort} from "../../utils/Utils";

const DENSIFICATION_NONE = '0';
const DENSIFICATION_MULTIPLICATION = '1';
const DENSIFICATION_INTERPOLATION = '2';

const DATE_TYPE = 'date';
const INTEGER_TYPE = 'integer';
const DOUBLE_TYPE = 'double';
const LONG_TYPE = 'long';

class IngestionModal extends Component {

  constructor(props) {
    super(props);

    this.state = {
      topic: 'consumptions',
      densificationType: DENSIFICATION_NONE,
      recordsPerSecond: 100000,
      densificationCount: 2,
      interpolators: {},
      interpolatorDateFormats: {},
      interpolationId: '',
      submitLoading: false,
      alertVisible: false,
      alertMessage: ''
    };
  }

  disableAlert = () => {
    this.setState({alertVisible: false, alertMessage: ''});
  };

  showAlert = (message) => {
    this.setState({alertVisible: true, alertMessage: message});
  };

  // submitIngestion = () => {
  //   const params = new URLSearchParams();
  //   params.append('topic', this.state.topic);
  //   params.append('recordsPerSecond', this.state.recordsPerSecond);
  //   params.append('densificationCount', this.state.densificationCount);
  //   params.append('densificationType', this.state.densificationType);
  //   params.append('datasetName', this.props.dataset.datasetName);
  //   console.log("URL: " + serverIpPort + '/ingestion?' + params.toString());
  //   fetch(serverIpPort + '/ingestion?' + params.toString(), {
  //     method: "POST",
  //   })
  //     .then(response => response.json())
  //     .then(data => {
  //       console.log(data);
  //       this.toggle();
  //     });
  // };

  isEmpty = (obj) => {
    for(let prop in obj) {
      if(obj.hasOwnProperty(prop))
        return false;
    }

    return true;
  }

  submitIngestion = () => {
    if(this.state.densificationType === DENSIFICATION_INTERPOLATION && this.isEmpty(this.state.interpolators)){
      console.log(this.state.interpolators);
      this.showAlert('At least one interpolator has to be specified!');
      return;
    }

    let interpolators = [];
    for(let index in this.state.interpolators) {
      let type = this.state.interpolators[index];
      if(type === DATE_TYPE){
        let dateFormat = this.state.interpolatorDateFormats[index] ? this.state.interpolatorDateFormats[index] : "yyyy-MM-dd HH:mm:ss";
        interpolators.push(index + ";" + type + ";" + dateFormat);
      }else{
        interpolators.push(index + ";" + type);
      }
    }

    const data = {
      topic: this.state.topic,
      recordsPerSecond: this.state.recordsPerSecond,
      interpolationId: this.state.interpolationId,
      densificationType: this.state.densificationType,
      densificationCount: this.state.densificationCount,
      datasetName: this.props.dataset.datasetName,
      interpolators: interpolators,
    };

    console.log(data);

    fetch(serverIpPort + '/ingestion', {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(data)
    })
      .then(response => response.json())
      .then(data => {
        console.log(data);
        this.toggle();
      })
  };


  topicChanged = (event) => {
    this.setState({topic: event.target.value});
    console.log(event.target.value);
  };

  setDefaults = () => {
    this.setState({
      topic: 'consumptions',
      densificationType: DENSIFICATION_NONE,
      recordsPerSecond: 100000,
      densificationCount: 2,
      interpolationId: '',
      interpolatorDateFormats: {},
      interpolators: {},
      alertVisible: false,
      alertMessage: ''
    });
  };

  toggle = () => {
    this.setDefaults();
    this.props.toggle();
  };


  densificationChanged = (event) => {
    this.setState({densificationType: event.target.value});
    console.log(event.target.value);
  };

  recordsPerSecondChanged = (event) => {
    this.setState({
      recordsPerSecond: event.target.value
    });
    console.log(event.target.value);
  };

  multiplicationCountChanged = (event) => {
    this.setState({
      densificationCount: event.target.value
    });
    console.log(event.target.value);
  };

  interpolatorChanged = (index, event) => {
    let interpolatorType = event.target.value;
    console.log("Interpolator type: " + interpolatorType + " index: " + index);
    console.log(this.state.interpolators);
    this.state.interpolators[index] = interpolatorType;
  };

  dateFormatChanged = (index, event) => {
    let dateFormat = event.target.value;
    console.log("Date format: " + dateFormat + " index: " + index);
    this.state.interpolatorDateFormats[index] = dateFormat;
  };

  idChanged = (event) => {
    let id = event.target.value;
    console.log("Id: " + id);
    this.state.interpolationId = id;
  };

  showInterpolationForm = () => {
    if (this.state.densificationType === DENSIFICATION_INTERPOLATION) {
      let columns = this.props.dataset.preview[0].split(',');

      return (
        <div>
          <FormGroup>
            <Label>Id index:</Label>
              <Input type="select" name="densification" id="densification" onChange={this.idChanged}>
                <option value='none' selected="selected">None</option>
                {columns.map((value, index) => {
                  return (
                    <option key={index} value={index}>{index}</option>
                  )})}
              </Input>
          </FormGroup>

          <strong style={{marginBottom: '5px'}}>Choose interpolators:</strong>
          {columns.map((value, index) => {
            return (
              <div key={index}>
                <FormGroup>
                  <Label>Index: {index}</Label>
                  <div style={{display: 'flex', flexDirection: 'row'}}>
                  <Input style={{width: '30%'}} type="select" name="densification" id="densification" onChange={this.interpolatorChanged.bind(this,index)}>
                    <option value='' selected="selected">None</option>
                    <option value={DOUBLE_TYPE}>Double</option>
                    <option value={INTEGER_TYPE}>Integer</option>
                    <option value={LONG_TYPE}>Long</option>
                    <option value={DATE_TYPE}>Date</option>
                  </Input>
                    {this.state.interpolators[index] && this.state.interpolators[index] === DATE_TYPE ?
                      <Input style={{marginLeft: '5px', width: '70%'}} type="text" name="dformat" id="dformat" placeholder="Date format [default: yyyy-MM-dd HH:mm:ss]" onChange={this.dateFormatChanged.bind(this.index)}/>
                      : null}
                  </div>
                </FormGroup>

              </div>
            )
          })}
        </div>
      );
    } else return null;
  };

  render() {
    if (!this.props.dataset) return null;
    else return (
      <Modal isOpen={this.props.isOpen} toggle={this.toggle}>
        <ModalHeader toggle={this.toggle}><h5>Ingest dataset {this.props.dataset.datasetName} <Badge
          color="success">{formatBytes(this.props.dataset.size)}</Badge></h5></ModalHeader>
        <ModalBody>

          <Alert color="danger" isOpen={this.state.alertVisible} toggle={this.disableAlert}>
            {this.state.alertMessage}
          </Alert>

          <Form>
            <FormGroup>
              <Label for="topic">Topic</Label>
              <Input type="text" name="topic" id="topic" placeholder="consumptions" onChange={this.topicChanged}/>
            </FormGroup>

            <FormGroup>
              <Label>Records per second</Label>
              <Input type="select" name="rps" id="rps" onChange={this.recordsPerSecondChanged}>
                <option value="50000">50 000 r/s</option>
                <option value="75000">75 000 r/s</option>
                <option value="100000" selected="selected">100 000 r/s</option>
                <option value="125000">125 000 r/s</option>
                <option value="150000">150 000 r/s</option>
                <option value="175000">175 000 r/s</option>
                <option value="200000">200 000 r/s</option>
                <option value="225000">225 000 r/s</option>
                <option value="250000">250 000 r/s</option>
                <option value="275000">275 000 r/s</option>
              </Input>
            </FormGroup>

            <FormGroup>
              <Label>Densification</Label>
              <Input type="select" name="densification" id="densification" onChange={this.densificationChanged}>
                <option value={DENSIFICATION_NONE} selected="selected">None</option>
                <option value={DENSIFICATION_MULTIPLICATION}>Multiply</option>
                <option value={DENSIFICATION_INTERPOLATION}>Interpolate</option>
              </Input>
            </FormGroup>

            {this.state.densificationType !== DENSIFICATION_NONE ?
              <FormGroup>
                <Label>Multiply count</Label>
                <Input type="number" name="mc" id="mc" value={this.state.densificationCount} min="2" max="1000"
                       onChange={this.multiplicationCountChanged}/>
              </FormGroup> : null}

            {this.showInterpolationForm()}

          </Form>
        </ModalBody>
        <ModalFooter>
          <Button color="primary" onClick={this.submitIngestion}>Run</Button>{' '}
          <Button color="secondary" onClick={this.toggle}>Cancel</Button>
        </ModalFooter>
        {/*{this.showIngestionModal}*/}
      </Modal>
    );
  }
}

export default IngestionModal;
