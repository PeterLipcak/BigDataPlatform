import React, {Component} from 'react';
import {
  Badge,
  Card,
  CardBody,
  Button,
  CardHeader,
  Col,
  Row,
  Modal,
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
import moment from 'moment';
import IngestionModal from "../../components/IngestionModal/IngestionModal";

class Ingestion extends Component {
  constructor(props) {
    super(props);

    this.state = {
      datasets: [],
      opacity: 1,
      modal: false,
      currentIndex: -1,
      alertVisible: false,
      alertMessage: '',
      files: [],
      ingestions: [],
    };


    this.toggle = this.toggle.bind(this);
    this.onDismiss = this.onDismiss.bind(this);
  }

  onDismiss() {
    this.setState({alertVisible: false});
  }

  toggle = (i) => {
    this.setState(prevState => ({
      modal: !prevState.modal,
      currentIndex: i
    }));

    console.log("current index: " + this.state.currentIndex);
  };

  componentDidMount() {
    this.fetchDatasets();
    this.fetchIngestions()
    this.datasetsInterval = setInterval(() => this.fetchDatasets(), 15000);
    this.ingestionInterval = setInterval(() => this.fetchIngestions(), 2000);
  }

  componentWillUnmount() {
    clearInterval(this.datasetsInterval);
    clearInterval(this.ingestionInterval);
  }

  fetchDatasets() {
    fetch(serverIpPort + '/datasets')
      .then(r => r.json().then(data => ({status: r.status, body: data})))
      .then(response => {
        if (response.status === 200) {
          console.log(response);
          this.setState({datasets: response.body.datasets})
        } else {
          this.setState({datasets: []})
        }
      });
  }

  fetchIngestions() {
    fetch(serverIpPort + '/ingestions')
      .then(r => r.json().then(data => ({status: r.status, body: data})))
      .then(response => {
        if (response.status === 200) {
          console.log(response);
          this.setState({ingestions: response.body.ingestions})
        } else {
          this.setState({ingestions: []})
        }
      });

    // .then(response => response.json())
    // .then(data => {
    //   console.log(data);
    //   this.setState({datasets: data.datasets})
    // });
  }


  deleteDatasetClicked = (index) => {
    let datasetToDelete = this.state.datasets[index];
    console.log("trash i=" + index);
    console.log("dataset to delete name = " + datasetToDelete.datasetName);
    fetch(serverIpPort + '/datasets?dataset=' + datasetToDelete.datasetName, {
      method: "DELETE",
    })
      .then(response => response.json())
      .then(data => {
        this.setState({alertMessage: data.message, alertVisible: true});
        this.fetchDatasets();
      });
  };

  cancelIngestion = (id) => {
    fetch(serverIpPort + '/ingestion?id=' + id, {
      method: "DELETE",
    })
      .then(response => response.json())
      .then(data => {
        // this.setState({alertMessage: data.message, alertVisible: true});
        this.fetchIngestions();
      });
  };

  getColorByStatus = (status) => {
    if(status === "RUNNING")return 'secondary';
    else if(status === "FINISHED")return 'success';
    else return 'danger';

  };

  isRunning = (status) => {
    return status === "RUNNING";
  };

  showRunningIngestionsTable = () => {
    return (
      <Table responsive>
        <thead>
        <tr>
          <th>Dataset</th>
          <th>Time submitted</th>
          <th>Status</th>
          <th>Actions</th>
        </tr>
        </thead>
        <tbody>

        {this.state.ingestions.map((ingestion, i) =>
          <tr key={i}>
            <td>{ingestion.datasetName}</td>
            <td>{moment(ingestion.startTime).format("DD MMM YYYY hh:mm a").toString()}</td>
            <td>
              <Badge color={this.getColorByStatus(ingestion.status)}>{ingestion.status}</Badge>
            </td>
            <td>
              {this.isRunning(ingestion.status)
                ?
                <i onClick={this.cancelIngestion.bind(this, ingestion.id)}
                   className="cui-trash icons pointer">
                </i>
                :
                <i className="cui-trash icons grey"/>
              }
            </td>
          </tr>)}

        </tbody>
      </Table>
    )
  };

  showRunningIngestions = () => {
    return (
      <Row>
        <Col xs="12" lg="6">
          <Card>
            <CardHeader>
              <i className="fa fa-align-justify"></i> Ingestions
            </CardHeader>
            <CardBody>

              {this.showRunningIngestionsTable()}

            </CardBody>
          </Card>
        </Col>
      </Row>
    )
  };

  showPreviews() {
    if (!this.state.datasets) return null;
    return (
      <Row>
        {this.state.datasets.map((dataset, i) =>
          <Col key={i} xs="12" sm="6" md="4">
            <Card>
              <CardHeader>
                {dataset.datasetName} <i onClick={this.deleteDatasetClicked.bind(this, i)}
                                         className="cui-trash icons pointer"/>
                <div className="card-header-actions">
                  <Badge color="success" className="float-right">{formatBytes(dataset.size)}</Badge>
                </div>
              </CardHeader>
              <CardBody className="cardBody pointer">
                <div onClick={this.toggle.bind(this, i)}>
                  {/*{dataset.preview.join(" \n\n ")}*/}
                  {dataset.preview.map((preview,i) => <p key={i}>{preview}</p>)}
                </div>
              </CardBody>
            </Card>

          </Col>
        )}
      </Row>
    )
  }

  render() {
    return (
      <div className="animated fadeIn">
        {/*<Dropzone onDrop={acceptedFiles => console.log(acceptedFiles)}>*/}
        {/*{({getRootProps, getInputProps}) => (*/}
        {/*<section>*/}
        {/*<div {...getRootProps()} className="fileUpload">*/}
        {/*<input {...getInputProps()} />*/}
        {/*<p>Drag 'n' drop some files here, or click to select files</p>*/}
        {/*</div>*/}
        {/*</section>*/}
        {/*)}*/}
        {/*</Dropzone>*/}

        {this.showRunningIngestions()}

        <FilePond ref={ref => this.pond = ref}
                  files={this.state.files}
                  allowMultiple={false}
                  server={serverIpPort + "/uploadFile"}
                  onupdatefiles={fileItems => {
                    // Set currently active file objects to this.state
                    this.setState({
                      files: fileItems.map(fileItem => fileItem.file)
                    });
                  }}>
        </FilePond>

        <Alert color="success" isOpen={this.state.alertVisible} toggle={this.onDismiss}>
          {this.state.alertMessage}
        </Alert>

        {this.showPreviews()}

        <IngestionModal isOpen={this.state.modal} dataset={this.state.datasets[this.state.currentIndex]}
                        toggle={this.toggle}/>

      </div>
    );
  }
}

export default Ingestion;
