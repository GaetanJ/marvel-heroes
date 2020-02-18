const csv = require("csv-parser");
const fs = require("fs");

const { Client } = require("@elastic/elasticsearch");

const heroesIndexName = "heroes";

async function run() {
  const client = new Client({ node: "http://localhost:9200" });
  let dataset = [];

  client.indices.create(
    {
      index: heroesIndexName
    },
    (err, resp) => {
      if (err) console.trace(err.message);
    }
  );
  // TODO il y a peut être des choses à faire ici avant de commencer ...

  // Read CSV file
  fs.createReadStream("all-heroes.csv")
    .pipe(
      csv({
        separator: ","
      })
    )
    .on("data", data => {
      // TODO ici on récupère les lignes du CSV ...
      dataset.push(data);
    })
    .on("end", async () => {
      // TODO il y a peut être des choses à faire à la fin aussi ?
      console.log(dataset[0]);

      try {
        await client.bulk(createBulkInsertQuery(dataset));
      } catch {
        console.log("errors");
      }

      client.close();
      console.log("Terminated!");
    });
}

function createBulkInsertQuery(heroes) {
  const body = heroes.reduce((acc, hero) => {
    acc.push({
      index: { _index: heroesIndexName, _type: "_doc", _id: hero.id }
    });
    delete hero.id;
    acc.push(hero);
    return acc;
  }, []);

  return { body };
}

run().catch(console.error);
