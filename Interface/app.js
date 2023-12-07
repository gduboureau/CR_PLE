const express = require('express');
const hbase = require('hbase');
const path = require('path');

const app = express();
const port = 3000;

// Configuration HBase
const hbaseClient = hbase({
  host: 'lsd-prod-namenode-0.lsd.novalocal',
  protocol: 'https',
  port: 8080,
  krb5: {
    service_principal: 'HTTP/lsd-prod-namenode-0.lsd.novalocal',
    principal: 'vloustau@LSD.NOVALOCAL',
  },
});

// Middleware pour les fichiers statiques
app.use(express.static(path.join(__dirname, 'public')));

// Définir le moteur de modèle EJS
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// Définir la route principale
app.get('/', (req, res) => {
  // Vous pouvez ajouter ici la logique pour récupérer les familles de colonnes et les rowkeys de votre table
  // Puis passez ces données à votre modèle EJS
  const families = ['family1', 'family2', 'family3']; // À remplacer par vos propres données
  const rowkeys = ['rowkey1', 'rowkey2', 'rowkey3']; // À remplacer par vos propres données

  res.render('index', { families, rowkeys });
});

// Middleware pour le traitement des données POST
app.use(express.urlencoded({ extended: true }));

// Définir la route pour gérer la requête POST
app.post('/getData', async (req, res) => {
  const { columnFamily, rowKey } = req.body;

  try {
    // Utilisez la colonne family et la rowkey pour obtenir les données de HBase
    const data = await getDataFromHBase(columnFamily, rowKey);
    res.render('result', { data });
  } catch (error) {
    console.error('Erreur lors de la récupération des données depuis HBase:', error);
    res.status(500).send('Erreur lors de la récupération des données depuis HBase');
  }
});

// Fonction pour obtenir les données de HBase
function getDataFromHBase(columnFamily, rowKey) {
  return new Promise((resolve, reject) => {
    hbaseClient
      .table('vloustau:CRdata') 
      .row(rowKey)
      .get(columnFamily, (error, value) => {
        if (error) {
          reject(error);
        } else {
          resolve(value);
        }
      });
  });
}

// Démarrer le serveur
app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
