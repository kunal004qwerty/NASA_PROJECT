const { parse } = require('csv-parse');
const { createReadStream } = require('node:fs');
const { Transform } = require('node:stream');
const { pipeline } = require('node:stream/promises');

const planets = require('./planets.mongo');

function isHabitablePlanet(planet) {
  return (
    planet['koi_disposition'] === 'CONFIRMED' &&
    planet['koi_insol'] > 0.36 &&
    planet['koi_insol'] <= 1.11 &&
    planet['koi_prad'] < 1.6
  );
}

async function loadPlanetData() {
  const fileName = './data/kepler_data.csv';
  const readStream = createReadStream(fileName);

  const parseStream = parse({
    comment: '#',
    columns: true,
  });

  const filterHabitablePlanets = new Transform({
    objectMode: true,
    async transform(chunk, enc, cb) {
      // Check if the planet is habitable
      if (isHabitablePlanet(chunk)) {
        await savePlanet(chunk); // Modularized the saving logic
      }
      cb(null); // Continue the stream without passing data downstream
    },
  });

  try {
    // Set up the pipeline
    await pipeline(readStream, parseStream, filterHabitablePlanets);

    console.log('Stream processing successfully completed');
  } catch (error) {
    console.error('Stream processing failed:', error);
  }
}

async function getAllPlanets() {
  return await planets.find(
    {},
    {
      __v: 0,
      _id: 0, // Removing internal MongoDB fields
    }
  );
}

async function savePlanet(data) {
  const { kepler_name, ...rest } = data;
  try {
    await planets.updateOne(
      { keplerName: kepler_name },
      { keplerName: kepler_name, ...rest },
      { upsert: true }
    );
    // console.log(`Planet ${kepler_name} saved successfully.`);
  } catch (error) {
    console.error(`Error updating planet ${kepler_name}:`, error);
  }
}

module.exports = {
  loadPlanetData,
  getAllPlanets,
};
