const express = require('express');
const hbase = require('hbase');
const path = require('path');
const{ Deck } = require('./deck'); 

const app = express();
const port = 3004;

// Configuration HBase
const hbaseClient = hbase({
  host: 'lsd-prod-namenode-0.lsd.novalocal',
  protocol: 'https',
  port: 8080,
  krb5: {
    service_principal: 'HTTP/lsd-prod-namenode-0.lsd.novalocal',
    principal: 'gduboureau@LSD.NOVALOCAL',
  },
});

// Fonction pour obtenir les données de HBase
function getDataFromHBase(columnFamily, rowKey) {
  return new Promise((resolve, reject) => {
    hbaseClient
      .table('gduboureau:CRdata') 
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

function getColumnDescription(columnName) {
  const columnDescriptions = {
    nb_uniquePlayer: "Nombre unique de joueur jouant le deck",
    best_clan : "Meilleur clan jouant le deck gagant",
    diff_force : "Différence moyenne de force du deck gagnant",
    nb_use : "Nombre d'utilisation du deck",
    nb_win : "Nombre de victoire du deck",
  };

  return columnDescriptions[columnName] || columnName;
}


async function getHBaseMetadata() {
  return new Promise((resolve, reject) => {
    hbaseClient
      .table('gduboureau:CRdata')
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
            .table('gduboureau:CRdata')
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
        <h1>Statistiques de Decks Clash Royale</h1>
        <h2>Cette interface vous permet d'afficher les meilleurs decks selon une statistique ainsi qu'une durée.</h2>
    
        <br>

        <form action="/getData" method="post">
          <label for="columnFamily">Choisissez la statisque :</label>
          <select name="columnFamily" id="columnFamily">
            ${families.map(family => `<option value="${family}">${family}</option>`).join('')}
          </select>
    
          <br><br><br>

          <label for="rowKey">Choisissez la période :</label>
          <select name="rowKey" id="rowKey">
            ${rowkeys.map(rowkey => `<option value="${rowkey}">${rowkey}</option>`).join('')}
          </select>
    
          <br><br><br><br><br>

          <button type="submit">Afficher les decks</button>
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
    const data = await getDataFromHBase(columnFamily, rowKey);

    const cardIdArray = [];
    const valueArray = [];
    data.forEach(item => {
      const columnParts = item.column.split(':'); 
      const cardId = columnParts[1]; 
      if (cardId && cardId.startsWith('cardId')) {
        cardIdArray.push({ cardId, value: item['$'] }); 
      } else if (cardId && cardId.startsWith('value')) {
        valueArray.push({ cardId, value: item['$'] }); 
      }
    });

    cardIdArray.sort((a, b) => {
      const numA = parseInt(a.cardId.split('_')[1]);
      const numB = parseInt(b.cardId.split('_')[1]);
      return numA - numB;
    });
    
    valueArray.sort((a, b) => {
      const numA = parseInt(a.cardId.split('_')[1]);
      const numB = parseInt(b.cardId.split('_')[1]);
      return numA - numB;
    });

    const selectedColumn = getColumnDescription(columnFamily) || "Statistique";

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
      <h1>${selectedColumn}</h1>
      <div>
      <ul>
        ${cardIdArray.map((item, index) => {
          const deck = new Deck(item.value);
          const card = deck.cards();
          return `
            <h3>Top ${index + 1}</h3>
            <li>Identifiant du deck: ${item.value}, 
            Valeur de la statistique: ${valueArray.find(v => v.cardId === 'value_' + item.cardId.split('_')[1]).value}</li>
            <div style="display:inline-block;">
              ${card.map(([name, imageUrl]) => `
                <div style="display:inline-block; margin-right:10px;">
                  <img src="${imageUrl}" alt="${name}" width="100" height="100">
                  <p>${name}</p>
                </div>`).join('')}
            </div>`;
        }).join('')}
      </ul>
    </div>
    </body>
    </html>
  `;

    // Renvoyer le HTML au client
    res.send(html);
  } catch (error) {
    console.error('Erreur lors de la récupération des données depuis HBase:', error);
    res.status(500).send("Aucune donnée n'a été trouvée pour cette période et cette statistique.");
  }
});

// Démarrer le serveur
app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
