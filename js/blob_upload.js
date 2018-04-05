export class BlobUpload {
  constructor(blob) {
    this.blob = blob
    this.file = blob.file

    const { url, headers } = blob.directUploadData

    this.xhr = new XMLHttpRequest
    this.xhr.open("POST", url, true)
    this.xhr.responseType = "text"
    this.xhr.addEventListener("load", event => this.requestDidLoad(event))
    this.xhr.addEventListener("error", event => this.requestDidError(event))
  }

  create(callback) {
    this.callback = callback
    const data = new FormData();
    const headers = this.blob.directUploadData.headers;
    for (const key in headers) {
      data.append(key, headers[key])
    }
    data.append("file", this.file.slice());
    this.xhr.send(data);
  }

  requestDidLoad(event) {
    const { status, response } = this.xhr
    if (status >= 200 && status < 300) {
      this.callback(null, response)
    } else {
      this.requestDidError(event)
    }
  }

  requestDidError(event) {
    this.callback(`Error storing "${this.file.name}". Status: ${this.xhr.status}`)
  }
}
