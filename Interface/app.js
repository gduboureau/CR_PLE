const express = require('express');
const hbase = require('hbase');
const path = require('path');
const { Deck } = require('./deck');

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
    best_clan: "Meilleur clan jouant le deck gagant",
    diff_force: "Différence moyenne de force du deck gagnant",
    nb_use: "Nombre d'utilisation du deck",
    nb_win: "Nombre de victoire du deck",
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
          hbaseClient
            .table('gduboureau:CRdata')
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

app.use(urlencoded({ extended: true }));

app.get('/', showHomePage);

app.post('/getData', async (req, res) => {
  const { columnFamily, rowKey, numDecks } = req.body;

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
    const numDecksToShow = parseInt(numDecks, 10) || cardIdArray.length;

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
      <h1>${selectedColumn}</h1>
      <div>
      <ul class="deck-ul">
        ${cardIdArray.slice(0, numDecksToShow).map((item, index) => {
      const deck = new Deck(item.value);
      const card = deck.cards();
      return `
          <div class="card-deck-div">
            <h3>Top ${index + 1}</h3>
            <li>Identifiant du deck: ${item.value}<li> 
            <p>Valeur de la statistique: ${valueArray.find(v => v.cardId === 'value_' + item.cardId.split('_')[1]).value}</p>
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
