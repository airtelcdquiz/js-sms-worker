const smpp = require('smpp');
const Queue = require('bull');

const REDIS_URL = 'redis://ussd-redis:6379';
const smsQueue = new Queue('sms_queue', REDIS_URL);

const SMPP_CONFIG = {
  host: 'messaging.airtel.cd',
  port: 9001,
  system_id: 'AirtelQuiz',
  password: '@irtElq1',
  source_addr: 'AirtelQuiz',
};

const PARALLEL_JOBS = 10;

let session;
let isBound = false;

function connectSmpp() {
  session = new smpp.Session({
    host: SMPP_CONFIG.host,
    port: SMPP_CONFIG.port,
    auto_enquire_link_period: 10000,
    connectTimeout: 20000,
  });

  session.on('connect', () => {
    console.log('SMPP connected, binding...');
    session.bind_transceiver({
      system_id: SMPP_CONFIG.system_id,
      password: SMPP_CONFIG.password,
    });
  });

  session.on('bind_transceiver_resp', (pdu) => {
    if (pdu.command_status === 0) {
      console.log('SMPP bind SUCCESS');
      isBound = true;
      startProcessing();
    } else {
      console.error('SMPP bind FAILED:', pdu.command_status);
      setTimeout(connectSmpp, 5000);
    }
  });

  session.on('close', () => {
    console.log('SMPP session closed');
    isBound = false;
    setTimeout(connectSmpp, 5000);
  });

  session.on('error', (err) => {
    console.error('SMPP error:', err);
    isBound = false;
  });
}

function sendSMS(job) {
  return new Promise((resolve, reject) => {
    if (!isBound) {
      return reject(new Error('SMPP not bound'));
    }

    const { phoneNumber, message } = job.data;

    session.submit_sm({
      source_addr: SMPP_CONFIG.source_addr,
      dest_addr_ton: 1,
      dest_addr_npi: 1,
      destination_addr: phoneNumber,
      short_message: message,
    }, (pdu) => {
      if (pdu.command_status === 0) {
        console.log(`SMS envoyé à ${phoneNumber}`);
        resolve();
      } else {
        reject(new Error(`SMPP error ${pdu.command_status}`));
      }
    });
  });
}

function startProcessing() {
  smsQueue.process(PARALLEL_JOBS, async (job) => {
    return sendSMS(job);
  });

  smsQueue.on('completed', (job) => {
    console.log(`Job ${job.id} completed`);
  });

  smsQueue.on('failed', (job, err) => {
    console.error(`Job ${job.id} failed:`, err.message);
  });
}

connectSmpp();
