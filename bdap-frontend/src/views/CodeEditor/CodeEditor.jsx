import React, {Component} from 'react';
import AceEditor from "react-ace";
import {split as SplitEditor} from 'react-ace';
import {BLANK_FLINK_TEMPLATE, DEFAULT_MVN_TEMPLATE} from './../../utils/CodeTemplates'
import brace from 'brace';
import {Button, Badge} from 'reactstrap';

// Import a Mode (language)
import 'brace/mode/java';
import 'brace/mode/xml';

// Import a Theme (okadia, github, xcode etc)
import 'brace/theme/monokai';
import 'brace/theme/twilight';
import 'brace/theme/kuroir';
import 'brace/theme/xcode';
import 'brace/theme/solarized_dark';
import {serverIpPort} from "../../utils/Utils";

class CodeEditor extends Component {

  constructor(props) {
    super(props);

    this.state = {
      code: BLANK_FLINK_TEMPLATE,
      dependencies: DEFAULT_MVN_TEMPLATE,
      saveLoading: false,
      compilationLoading: false,
      submitLoading: false,
      projectName: 'Simple anomaly detection',

      outputTitle: '',
      outputText: '',
      outputBadgeText: '',
      outputBadgeColor: '',
      outputOverallSuccess: false,
      outputVisible: false,
      submitting: false
    };

    // this.onCodeChange = this.onCodeChange.bind(this);
    // this.onDependenciesChange = this.onDependenciesChange.bind(this);
    // this.onSave = this.onSave.bind(this);
    // this.onCompile = this.onCompile.bind(this);
    this.handleKeyDown = this.handleKeyDown.bind(this);
  }

  handleKeyDown(event) {
    let charCode = String.fromCharCode(event.which).toLowerCase();
    if(event.ctrlKey && charCode === 's' || event.metaKey && charCode === 's') {
      event.preventDefault();
      console.log("Ctrl + S pressed");
      this.onSave();
    }
  }

  setOutput = (outputTitle, outputText, outputBadgeText, outputBadgeColor, outputOverallSuccess, outputVisible) => {
    this.setState({
      outputTitle: outputTitle,
      outputText: outputText,
      outputBadgeText: outputBadgeText,
      outputBadgeColor: outputBadgeColor,
      outputOverallSuccess: outputOverallSuccess,
      outputVisible: outputVisible
    });
  };


  cleanResults = () => {
    this.setState({
      outputTitle: '',
      outputText: '',
      outputBadgeText: '',
      outputBadgeColor: '',
      outputOverallSuccess: false,
      outputVisible: false,
      submitting: false
    });
  };

  onCodeChange = (newValue) => {
    this.setState({
      code: newValue
    })
  };

  onDependenciesChange = (newValue) => {
    this.setState({
      dependencies: newValue
    })
  };


  onSave = () => {
    this.cleanResults();
    console.log(this.state);
    this.setState({
      saveLoading: true
    });

    const data = {
      projectName: this.state.projectName,
      code: this.state.code,
      dependencies: this.state.dependencies
    };

    fetch(serverIpPort + '/saveCode', {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(data)
    })
      .then(response => response.json())
      .then(data => {
        console.log(data);
        this.setState({
          saveLoading: false,
        });
      });
  };

  onCompile = () => {
    this.cleanResults();
    this.setState({
      compilationLoading: true,
    });

    const data = {
      projectName: this.state.projectName,
      code: this.state.code,
      dependencies: this.state.dependencies
    };

    fetch(serverIpPort + '/compileCode', {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(data)
    })
      .then(response => response.json())
      .then(data => {
        console.log(data);
        this.setState({
          compilationLoading: false,
        });

        if(data.success){
          this.setOutput('Compilation output:',data.compilationOutput,'Build successfull','success',true,true);
        }else{
          this.setOutput('Compilation output:',data.compilationOutput,'Build failed','danger',false,true);
        }

        this.outputTextDiv.scrollIntoView({behavior: "smooth"});
      });
  };

  onSubmit = () => {
    this.cleanResults();
    this.setState({
      submitLoading: true,
    });

    const data = {
      projectName: this.state.projectName,
      code: this.state.code,
      dependencies: this.state.dependencies
    };

    fetch(serverIpPort + '/submitCode', {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(data)
    })
      .then(response => response.json())
      .then(data => {
        console.log(data);

        this.setState({
          submitLoading: false,
          submitting: true
        });

        if(!data.compilationSuccess){
          this.setOutput('Compilation output:',data.compilationResult.compilationOutput,'Build failed','danger',false,true);
        }else if(!data.submissionSuccess){
          this.setOutput('Submission output:',data.submissionOutput,'Submission failed','danger',false,true);
        }else{
          this.setOutput('Submission output:',data.submissionOutput,'Submission successful','success',true,true);
        }

        this.outputTextDiv.scrollIntoView({behavior: "smooth"});
      });
  };

  areButtonsDisabled = () => {
    return this.state.submitLoading || this.state.compilationLoading || this.state.saveLoading;
  };

  render() {
    return (
      <div onKeyDown={this.handleKeyDown} className="animated fadeIn codeEditorDivContainer">
        <div className="submitButtonContainer">
          <h3 style={{float: 'left'}}><strong>{this.state.projectName}</strong></h3>
          <Button onClick={this.onSubmit} className="editorButtonStyle" disabled={this.areButtonsDisabled()}
                  color="primary">{this.state.submitLoading ? 'Submitting...' : 'Submit'}</Button>
          <Button onClick={this.onCompile} className="editorButtonStyle" color="dark" disabled={this.areButtonsDisabled()}
                  aria-pressed="true">{this.state.compilationLoading ? 'Compiling...' : 'Compile'}</Button>
          <Button onClick={this.onSave} className="editorButtonStyle" color="success" disabled={this.areButtonsDisabled()}
                  aria-pressed="true">{this.state.saveLoading ? 'Saving...' : 'Save'}</Button>
        </div>

        <div className="codeEditorContainer">
          <AceEditor
            mode="java"
            theme="monokai"
            height='800px'
            width='50%'
            fontSize='14'
            onChange={this.onCodeChange}
            value={this.state.code}
            name="UNIQUE_ID_OF_DIV"
            editorProps={{
              $blockScrolling: true
            }}
            setOptions={{
              enableBasicAutocompletion: true,
              enableLiveAutocompletion: true,
              enableSnippets: false,
              showLineNumbers: true,
              tabSize: 2,
            }}
          />

          <AceEditor
            placeholder="pom.xml"
            mode="xml"
            theme="monokai"
            height='800px'
            width='50%'
            fontSize='14'
            onChange={this.onDependenciesChange}
            value={this.state.dependencies}
            name="id"
            editorProps={{
              $blockScrolling: true
            }}
            setOptions={{
              enableBasicAutocompletion: true,
              enableLiveAutocompletion: true,
              enableSnippets: false,
              showLineNumbers: true,
              tabSize: 2,
            }}
          />
        </div>

        <br/>
        {this.state.outputVisible ?
          <div ref={(el) => {
            this.outputTextDiv = el;
          }}>
              <h3 >{this.state.outputTitle}</h3>
            {this.state.submitting ? <Badge style={{marginBottom: '5px', marginRight: '5px'}}
                                            color='success'>Compilation successful</Badge> : null}
              <Badge style={{marginBottom: '5px'}}
                     color={this.state.outputBadgeColor}>{this.state.outputBadgeText}</Badge>
            <div style={{padding: '10px', backgroundColor: 'white'}}>
              <span style={{ backgroundColor: 'white', whiteSpace: 'pre-wrap'}}>{this.state.outputText}</span>
            </div>
          </div>
          :
          <div ref={(el) => {
            this.outputTextDiv = el;
          }}>
          </div>
        }

      </div>
    );
  }
}

export default CodeEditor;
