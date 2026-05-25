const createReq = ({ body = {}, params = {}, query = {} } = {}) => ({
  body,
  params,
  query
});

const createRes = () => {
  const headers = {};
  const res = {
    statusCode: 200,
    headers,
    body: null,
    headersSent: false,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
    setHeader(key, value) {
      headers[key] = value;
      this.headersSent = true;
    },
    end() {
      this.ended = true;
    }
  };
  return res;
};

export { createReq, createRes };
