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
  Input
} from 'reactstrap';
import {formatBytes} from './../../utils/Utils';
import {serverIpPort} from "../../utils/Utils";

const DENSIFICATION_NONE = '1';
const DENSIFICATION_MULTIPLICATION = '2';
const DENSIFICATION_INTERPOLATION = '3';

class IngestionModal extends Component {

  constructor(props) {
    super(props);

    this.state = {
      topic: 'consumptions',
      densification: 1,
      recordsPerSecond: 100000,
      multiplicationCount: 2,
      interpolators: []
    };
  }

  submitIngestion = () => {
    const params = new URLSearchParams();
    params.append('topic', this.state.topic);
    params.append('recordsPerSecond', this.state.recordsPerSecond);
    params.append('multiplicationCount', this.state.multiplicationCount);
    params.append('densificationType', this.state.densification);
    params.append('datasetName', this.props.dataset.datasetName);
    console.log("URL: " + serverIpPort + '/ingestion?' + params.toString());
    fetch(serverIpPort + '/ingestion?' + params.toString(), {
      method: "POST",
    })
      .then(response => response.json())
      .then(data => {
        console.log(data);
      });
  };

  topicChanged = (event) => {
    this.setState({topic: event.target.value});
    console.log(event.target.value);
  };

  setDefaults = () => {
    this.setState({
      topic: 'consumptions',
      densification: 1,
      recordsPerSecond: 100000,
      multiplicationCount: 2,
      interpolators: []
    });
  };

  toggle = () => {
    this.setDefaults();
    this.props.toggle();
  };


  densificationChanged = (event) => {
    this.setState({densification: event.target.value});
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
      multiplicationCount: event.target.value
    });
    console.log(event.target.value);
  };

  showInterpolationForm = () => {
    if (this.state.densification === DENSIFICATION_INTERPOLATION) return null;
    // (
    //   <FormGroup>
    //     <Label>Multiply count</Label>
    //     <Input type="number" name="mc" id="mc" placeholder="2" min="2" max="100"
    //            onChange={this.multiplicationCountChanged}/>
    //   </FormGroup>
    // );
    else return null;
  };

  render() {
    let {multiplicationCount} = this.state;

    if (!this.props.dataset) return null;
    else return (
      <Modal isOpen={this.props.isOpen} toggle={this.toggle}>
        <ModalHeader toggle={this.toggle}><h5>Ingest dataset {this.props.dataset.datasetName} <Badge
          color="success">{formatBytes(this.props.dataset.size)}</Badge></h5></ModalHeader>
        <ModalBody>

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
                <option value="100000">100 000 r/s</option>
                <option value="125000">125 000 r/s</option>
                <option value="150000" selected="selected">150 000 r/s</option>
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
                <option value={DENSIFICATION_NONE}>None</option>
                <option value={DENSIFICATION_MULTIPLICATION}>Multiply</option>
                <option value={DENSIFICATION_INTERPOLATION}>Interpolate</option>
              </Input>
            </FormGroup>

            {this.state.densification === DENSIFICATION_MULTIPLICATION ?
              <FormGroup>
                <Label>Multiply count</Label>
                <Input type="number" name="mc" id="mc" value={multiplicationCount} min="2" max="100"
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
