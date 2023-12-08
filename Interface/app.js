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


async function getHBaseMetadata() {
  return new Promise((resolve, reject) => {
    hbaseClient
      .table('vloustau:CRdata')
      .schema((error, schema) => {
        if (error) {
          reject(error);
        } else {
          const familyName = [];
          for (const column of schema.ColumnSchema) {
            familyName.push(column.name);
          }

          // Récupérer quelques rowkeys à titre d'exemple
          hbaseClient
            .table('vloustau:CRdata')
            .scan({
              limit: 3,
            }, (error, rows) => {
              if (error) {
                reject(error);
              } else {
                const rowSet = new Set();

                // Ajouter les rowkeys à l'ensemble
                rows.forEach(row => {
                  rowSet.add(row.key.toString('utf8'));
                });

                // Convertir l'ensemble en tableau
                const rowkeys = Array.from(rowSet);


                // Résoudre la promesse avec les données
                resolve({ families: familyName, rowkeys });
              }
            });
        }
      });
  });
}

// Fonction pour afficher la page principale
async function showHomePage(req, res) {
  try {
    // Utiliser la fonction pour obtenir les données de HBase
    const { families, rowkeys } = await getHBaseMetadata();

    // Générer le HTML de la page principale
    const html = `
      <!DOCTYPE html>
      <html lang="en">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Interface HBase</title>
      </head>
      <body>
        <h1>Interface HBase</h1>
    
        <form action="/getData" method="post">
          <label for="columnFamily">Choisir la colonne family :</label>
          <select name="columnFamily" id="columnFamily">
            ${families.map(family => `<option value="${family}">${family}</option>`).join('')}
          </select>
    
          <label for="rowKey">Choisir la rowkey :</label>
          <select name="rowKey" id="rowKey">
            ${rowkeys.map(rowkey => `<option value="${rowkey}">${rowkey}</option>`).join('')}
          </select>
    
          <button type="submit">Afficher les données</button>
        </form>
      </body>
      </html>
    `;

    // Renvoyer le HTML au client
    res.send(html);
  } catch (error) {
    console.error('Erreur lors de la récupération des données depuis HBase:', error);
    res.status(500).send('Erreur lors de la récupération des données depuis HBase');
  }
}

// Middleware pour le traitement des données POST
app.use(express.urlencoded({ extended: true }));

// Route pour afficher la page principale
app.get('/', showHomePage);

// Route pour gérer la requête POST
app.post('/getData', async (req, res) => {
  const { columnFamily, rowKey } = req.body;

  try {
    // Utilisez la colonne family et la rowkey pour obtenir les données de HBase
    const data = await getDataFromHBase(columnFamily, rowKey);

    // Générer le HTML avec les données
    const html =  `
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Résultat</title>
    </head>
    <body>
      <h1>Résultat</h1>
  
      <div>
        ${data.map(item => `<p>${item}</p>`).join('')}
      </div>
    </body>
    </html>
  `;

    // Renvoyer le HTML au client
    res.send(html);
  } catch (error) {
    console.error('Erreur lors de la récupération des données depuis HBase:', error);
    res.status(500).send('Erreur lors de la récupération des données depuis HBase');
  }
});

// Démarrer le serveur
app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
