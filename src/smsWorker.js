const smpp = require('smpp');
const Queue = require('bull');
const REDIS_URL = 'redis://ussd-redis:6379';

// Création de la queue
const smsQueue = new Queue('sms_queue', REDIS_URL);

const smppConfig = {
    host: 'messaging.airtel.cd',  // Adresse du serveur SMPP
    port: 9001,               // Port du serveur SMPP
    system_id: 'AirtelQuiz',   // Identifiant système SMPP
    password: '@irtElq1',     // Mot de passe
    source_addr: 'AirtelQuiz',   // ID de l'expéditeur
  };

// Config SMPP
const SMPP_HOST = 'messaging.airtel.cd';
const SMPP_PORT = 9001;
const SMPP_SYSTEM_ID = 'AirtelQuiz';
const SMPP_SOURCE_ADDR = 'AirtelQuiz';
const SMPP_PASSWORD = '@irtElq1';
const PARALLEL_JOBS = 10;

// Connexion SMPP
const session = new smpp.Session({ host: 'messaging.airtel.cd', port: 9001, debug: true, auto_enquire_link_period: 10000, connectTimeout: 20000 });

  session.on('connect', () => {
    console.log(`Session SMPP connectée`);
    session.bind_transceiver({
      system_id: smppConfig.system_id,
      password: smppConfig.password
    });
  });

  session.on('bind_transceiver', (pdu) => {
    if (pdu.command_status === 0) {
      console.log(`Session SMPP liée avec succès`);
    }
  });

  session.on('close', () => {
    console.log(`Session SMPP fermée`);
    // Réessayer de reconnecter la session
    reconnectSession(session, i);
  });

  session.on('error', (err) => {
    console.log(`Erreur sur la session SMPP:`, err);
    // Réessayer de reconnecter la session
    reconnectSession(session, i);
  });

  smppSessions.push(session);


function bindSmpp() {
  session.bind_transceiver({
    system_id: SMPP_SYSTEM_ID,
    password: SMPP_PASSWORD,
  }, (pdu) => {
    if (pdu.command_status === 0) {
      console.log('SMPP connected successfully');
      startProcessing();
    } else {
      console.error('SMPP bind failed, retrying in 5s...');
      setTimeout(bindSmpp, 5000);
    }
  });
}

// Envoyer un SMS
function sendSMS(job) {
  return new Promise((resolve, reject) => {
    const { phoneNumber, message, meta } = job.data;

    session.submit_sm({
      source_addr: SMPP_SOURCE_ADDR,
      destination_addr: phoneNumber,
      short_message: message,
    }, (pdu) => {
      if (pdu.command_status === 0) {
        console.log(`SMS envoyé: ${phoneNumber} (jobId=${job.id})`);
        resolve();
      } else {
        console.error(`Erreur SMS: ${phoneNumber} (jobId=${job.id})`, pdu);
        reject(new Error(`SMPP error code ${pdu.command_status}`));
      }
    });
  });
}

// Lancer le worker Bull
function startProcessing() {
  smsQueue.process(PARALLEL_JOBS, async (job) => {
    return sendSMS(job);
  });

  smsQueue.on('completed', (job) => {
    console.log(`Job ${job.id} terminé avec succès`);
  });

  smsQueue.on('failed', (job, err) => {
    console.error(`Job ${job.id} échoué:`, err.message);
  });
}

// Lancer le bind SMPP
bindSmpp();
