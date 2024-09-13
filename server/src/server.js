const http = require('http');
require('dotenv').config()

const { app } = require('./app');
const { loadPlanetData } = require('./models/planets.model');
const { mongoConnect } = require('./services/mongo')
const { loadLaunchData } = require('./models/launches.model')

const PORT = process.env.PORT || 8000;

const server = http.createServer(app);


async function startServer() {
    // connect database
    await mongoConnect()

    // Load planet data before starting the server
    await loadPlanetData();

    await loadLaunchData()

    // Start the server
    server.listen(PORT, () => {
        console.log(`Listening on PORT ${PORT}`);
    });
}

startServer();
