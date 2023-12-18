const express = require('express');
const bodyParser = require('body-parser');
const hbase = require('hbase');
const path = require('path');
const { Deck } = require('./deck');
const { table } = require('console');

const app = express();
const port = 3002;

const username = 'vloustau';


// Configuration HBase
const hbaseClient = hbase({
  host: 'lsd-prod-namenode-0.lsd.novalocal',
  protocol: 'https',
  port: 8080,
  krb5: {
    service_principal: 'HTTP/lsd-prod-namenode-0.lsd.novalocal',
    principal: username+'@LSD.NOVALOCAL',
  },
});

// Fonction pour obtenir les données de HBase
function getDataFromHBase(columnFamily, rowKey) {
  return new Promise((resolve, reject) => {
    hbaseClient
      .table(username+':CRdata')
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

// Fonction pour obtenir les données de HBase
async function getHBaseMetadata() {
  return new Promise((resolve, reject) => {
    hbaseClient
      .table(username+':CRdata')
      .schema((error, schema) => {
        if (error) {
          reject(error);
        } else {
          // On récupère les familles de colonnes
          const familyName = [];
          for (const column of schema.ColumnSchema) {
            familyName.push(column.name);
          }
          hbaseClient
            .table(username+':CRdata') 
            .scan({
              limit: 3,
            }, (error, rows) => {
              if (error) {
                reject(error);
              } else {
                const rowSet = new Set();

                rows.forEach(row => {
                  rowSet.add(row.key.toString('utf8'));
                });

                const rowkeys = Array.from(rowSet); 


                resolve({ families: familyName, rowkeys }); // On retourne les familles de colonnes et les rowkeys
              }
            });
        }
      });
  });
}

// Fonction pour afficher la page principale
async function showHomePage(req, res) {
  try {
    const { families, rowkeys } = await getHBaseMetadata();

    const html = `
      <!DOCTYPE html>
      <html lang="en">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Interface HBase</title>
        <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;700&display=swap">
        <style>
          body {
            font-family: 'Poppins', sans-serif;
          }
      </style>
      </head>
      <body>
        <h1>Statistiques de Decks Clash Royale</h1>
        <h2>Cette interface vous permet d'afficher les meilleurs decks selon une statistique ainsi qu'une durée.</h2>
    
        <br>

        <form action="/getData" method="post">
          <label for="columnFamily">Choisissez la statistique :</label>
          <select name="columnFamily" id="columnFamily">
            ${families.map(family => `<option value="${family}">${family}</option>`).join('')}
          </select>
    
          <br><br><br>

          <label for="rowKey">Choisissez la période :</label>
          <select name="rowKey" id="rowKey">
            ${rowkeys.map(rowkey => `<option value="${rowkey}">${rowkey}</option>`).join('')}
          </select>

          <br><br><br>

          <label for="numDecks">Nombre de decks à afficher :</label>
          <select name="numDecks" id="numDecks">
            <option value="5">5</option>
            <option value="10">10</option>
            <option value="50">50</option>
          </select>
    
          <br><br><br>
  
          <button type="submit">Afficher les decks</button>
        </form>
      </body>
      </html>
    `;

    res.send(html);
  } catch (error) {
    console.error('Erreur lors de la récupération des données depuis HBase:', error);
    res.status(500).send('Erreur lors de la récupération des données depuis HBase');
  }
}

app.use(bodyParser.urlencoded({ extended: true })); // Pour pouvoir récupérer les données du formulaire

app.get('/', showHomePage); // Page principale

app.post('/getData', async (req, res) => {
  const { columnFamily, rowKey, numDecks } = req.body; // On récupère les paramètres du formulaire

  // On vérifie que les paramètres sont bien présents
  try {
    const data = await getDataFromHBase(columnFamily, rowKey);
    const dataArray = [];
    data.forEach(item => {
      const columnParts = item.column.split(':');
      const cardId = columnParts[1];
      const value = item['$'];
  
      if (cardId && value) {
        dataArray.push({ cardId, value: parseFloat(value) });
      }
    });
  
    // Trier le tableau d'objets en fonction des valeurs (de manière décroissante)
    dataArray.sort((a, b) => b.value - a.value);

    const numDecksToShow = parseInt(numDecks, 10) || dataArray.length;

    const html = `
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Résultat</title>
      <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;700&display=swap">
      <style>
        body {
          font-family: 'Poppins', sans-serif;
        }
      </style>
    </head>
    <body>
      <h1>Top ${numDecksToShow} des decks selon le ${columnFamily} pour la période ${rowKey}</h1>
      <div>
        <ul class="deck-ul">
          ${dataArray.slice(0, numDecksToShow).map((item, index) => {
            const deck = new Deck(item.cardId);
            const card = deck.cards();
            return `
              <div class="card-deck-div">
                <h3>Top ${index + 1}</h3>
                <li>Identifiant du deck: ${item.cardId}<li> 
                <p>Valeur de la statistique: ${item.value}</p>
                <div class="deck-cards">
                  ${card.map(([name, imageUrl]) => `
                    <div style="display:inline-block; margin-right:10px;">
                      <img src="${imageUrl}" alt="${name}" width="100" height="100">
                      <p>${name}</p>
                    </div>`).join('')}
                </div>
              </div>`;
          }).join('')}
        </ul>
      </div>
    </body>
    </html>
  `;

    res.send(html);
  } catch (error) {
    console.error('Erreur lors de la récupération des données depuis HBase:', error);
    res.status(500).send("Aucune donnée n'a été trouvée pour cette période et cette statistique.");
  }
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`); 
});
